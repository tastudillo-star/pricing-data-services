from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Callable, Tuple, Union, Iterable

import pandas as pd


# ======================================================================================
# Helpers (IDs canónicos)
# ======================================================================================
def canon_id(x: Any) -> str:
    """
    Canoniza IDs para comparar/matchear de forma estable:
      - strip
      - elimina sufijo .0 típico de floats serializados
    """
    if x is None:
        return ""
    if isinstance(x, float) and pd.isna(x):
        return ""
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def load_sku_value_map_from_csv(
    mapping_csv_path: str,
    *,
    comp_id_col: str = "comp_id",
    chiper_id_col: str = "best_chiper_id",
    status_col: str = "status",
    statuses: Optional[Union[str, Iterable[str]]] = ("AUTO",),
) -> dict:
    """
    Lee mapping_out.csv y retorna un dict listo para usar en value_maps["sku"].

    Resultado:
      { "<competidor_id>": "<sku_chiper>" }

    Importante:
      - Todo se maneja como STRING canónico (NO int) para evitar:
        ceros a la izquierda, .0, espacios, etc.
    """
    df = pd.read_csv(mapping_csv_path, dtype=str)

    required = {comp_id_col, chiper_id_col}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"mapping_csv no tiene columnas {sorted(missing)}. Tiene: {list(df.columns)}"
        )

    # Filtrado por status (si existe la columna y statuses no es None)
    if statuses is not None and status_col in df.columns:
        if isinstance(statuses, str):
            statuses_set = {statuses}
        else:
            statuses_set = set(statuses)
        df = df[df[status_col].isin(statuses_set)].copy()

    # Limpieza base
    df = df.dropna(subset=[comp_id_col, chiper_id_col]).copy()

    # Canonización (keys + values)
    df[comp_id_col] = df[comp_id_col].map(canon_id)
    df[chiper_id_col] = df[chiper_id_col].map(canon_id)

    # Filtra vacíos
    df = df[(df[comp_id_col] != "") & (df[chiper_id_col] != "")].copy()

    # Un key por comp_id (primera ocurrencia)
    df = df.drop_duplicates(subset=[comp_id_col], keep="first").copy()

    return dict(zip(df[comp_id_col].tolist(), df[chiper_id_col].tolist()))


# ======================================================================================
# Schemas SOLO para competidores + normalización especializada (sin I/O)
# ======================================================================================
ValueMap = Union[Dict[Any, Any], Callable[[Any], Any]]


class BaseSchema:
    """
    Estructura base:
      - df_schema: define columnas destino + dtypes esperados
      - map_dict:  {col_destino: col_origen}
      - value_maps (opcional): {col_destino: dict|callable} para recodificar valores
    """
    def __init__(self) -> None:
        self.df_schema: pd.DataFrame = pd.DataFrame()
        self.map_dict: Dict[str, str] = {}
        self.value_maps: Dict[str, ValueMap] = {}

        # Opcional: columnas que quieres canonizar como IDs (por defecto sku)
        self.canon_cols: set[str] = {"sku"}

    def to_json_list(self, df: pd.DataFrame):
        json_str = df.to_json(orient="records", date_format="iso")
        return json.loads(json_str)

    def apply_value_maps(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica recodificación por columna:
          - si mapping es dict: replace exacto
          - si mapping es callable: map(func)
        No falla si la columna no existe.
        """
        if not getattr(self, "value_maps", None):
            return df

        out = df.copy()
        for col, mapping in self.value_maps.items():
            if col not in out.columns:
                continue

            if callable(mapping):
                out[col] = out[col].map(mapping)  # type: ignore[arg-type]
            elif isinstance(mapping, dict) and mapping:
                out[col] = out[col].replace(mapping)
        return out


# ======================================================================================
# Normalización especializada para esta aplicación (competidores)
# ======================================================================================
def _normalize_text_series(s: pd.Series) -> pd.Series:
    """
    Normalización de texto ligera (vectorizada):
      - strip
      - colapsa espacios múltiples
    """
    s2 = s.astype("string")
    s2 = s2.str.strip()
    s2 = s2.str.replace(r"\s+", " ", regex=True)
    return s2


def normalize_dataframe(
    df: pd.DataFrame,
    schema: BaseSchema,
    normalice: bool = True,
    drop_na: bool = True,
    drop_duplicates: bool = False,
) -> Tuple[pd.DataFrame, Optional[list[Dict[str, Any]]]]:
    """
    Normaliza un DataFrame en memoria según un Schema de competidor (especializada).

    Pipeline:
      1) select + rename según map_dict
      2) asegurar columnas esperadas
      3) canonizar IDs (sku por defecto) ANTES del value_maps (para que matcheen las keys)
      4) apply_value_maps
      5) canonizar IDs de nuevo (por si el mapping trae .0 / espacios)
      6) cast tipos (suave)
      7) normalización texto (no toca sku)
      8) limpieza opcional
    """
    # DataFrame vacío con columnas/dtypes del schema
    if df is None or df.empty:
        out = pd.DataFrame({c: pd.Series(dtype=t) for c, t in schema.df_schema.dtypes.items()})
        return out, None

    expected_cols = list(schema.df_schema.columns)
    dtype_map = schema.df_schema.dtypes.to_dict()

    # 1) Select + rename (solo columnas presentes)
    present_pairs = [(dest, src) for dest, src in schema.map_dict.items() if src in df.columns]
    if not present_pairs:
        out = pd.DataFrame({c: pd.Series(dtype=dtype_map.get(c, "object")) for c in expected_cols})
        return out, None

    src_cols = [src for _, src in present_pairs]
    rename_map = {src: dest for dest, src in present_pairs}
    data = df.loc[:, src_cols].rename(columns=rename_map).copy()

    # 2) Asegurar columnas esperadas (forma estable)
    for c in expected_cols:
        if c not in data.columns:
            data[c] = pd.NA
    data = data[expected_cols]

    # 3) Canoniza IDs antes del value map (clave para que replace() haga match)
    for c in getattr(schema, "canon_cols", set()):
        if c in data.columns:
            data[c] = data[c].map(canon_id)

    # 4) Value maps
    data = schema.apply_value_maps(data)

    # 5) Canoniza IDs después del value map (evita que quede "123.0" o con espacios)
    for c in getattr(schema, "canon_cols", set()):
        if c in data.columns:
            data[c] = data[c].map(canon_id)

    # 6) Cast suave
    data = data.astype(dtype_map, errors="ignore")

    # 7) Normalización texto (no toca sku)
    if normalice:
        for col in data.columns:
            if col in getattr(schema, "canon_cols", set()):
                continue
            if pd.api.types.is_object_dtype(data[col]) or pd.api.types.is_string_dtype(data[col]):
                data[col] = _normalize_text_series(data[col])

    # 8) Limpieza
    if drop_na:
        data = data.dropna()
    if drop_duplicates:
        data = data.drop_duplicates()

    return data.reset_index(drop=True), None


# ======================================================================================
# Schemas competidores
# ======================================================================================
class GenericCompetidorSchema(BaseSchema):
    """
    Fallback si un competidor no tiene schema dedicado.
    """
    def __init__(self) -> None:
        super().__init__()
        # sku como texto (robusto para EAN con ceros a la izquierda / ids tipo string)
        self.df_schema = pd.DataFrame({
            "sku": pd.Series(dtype="object"),
            "fecha": pd.Series(dtype="datetime64[ns]"),
            "precio_lleno": pd.Series(dtype="float64"),
            "precio_descuento": pd.Series(dtype="float64"),
        })
        self.map_dict = {
            "sku": "product_ean",
            "fecha": "extraction_date",
            "precio_lleno": "product_price",
            "precio_descuento": "product_discount_price",
        }
        self.value_maps = {}


class CentralMayoristaSchema(GenericCompetidorSchema):
    def __init__(self) -> None:
        super().__init__()
        self.map_dict = {
            "sku": "product_ean",
            "fecha": "extraction_date",
            "precio_lleno": "product_price",
            "precio_descuento": "product_discount_price",
        }


class AlviSchema(GenericCompetidorSchema):
    def __init__(self) -> None:
        super().__init__()
        self.map_dict = {
            "sku": "product_ean",
            "fecha": "extraction_date",
            "precio_lleno": "product_price",
            "precio_descuento": "product_min_price",
        }


class AdelcoSchema(GenericCompetidorSchema):
    def __init__(self) -> None:
        super().__init__()
        self.map_dict = {
            "sku": "product_ean",
            "fecha": "extraction_date",
            "precio_lleno": "product_price",
            "precio_descuento": "product_discount_price",
        }


class LaOfertaSchema(GenericCompetidorSchema):
    def __init__(self) -> None:
        super().__init__()
        self.map_dict = {
            "sku": "product_sku",          # ID competidor (no necesariamente EAN)
            "fecha": "extraction_date",
            "precio_lleno": "product_price",
            # "precio_descuento": "product_discount_price",
        }

        # Path robusto: relativo a utils/
        mapping_path = "./utils/mapping_out.csv"

        sku_map = load_sku_value_map_from_csv(
            mapping_path,
            statuses=("AUTO", "REVIEW"),
        )
        self.value_maps = {"sku": sku_map}
