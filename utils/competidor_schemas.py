from __future__ import annotations

import json
from typing import Any, Dict, Optional, Callable, Tuple, Union

import pandas as pd


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
    Normalización de texto ligera y rápida (vectorizada):
      - strip
      - colapsa espacios múltiples
    NO fuerza upper/lower para no romper nombres de marca/categoría si no quieres.
    Si quieres, puedes cambiar a .str.lower() o .str.upper() aquí.
    """
    # Asegura string, preserva NaN
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
    Normaliza un DataFrame en memoria según un Schema de competidor.
    Especializada para este caso:
      - Selecciona solo columnas mapeadas y existentes
      - Renombra a columnas destino
      - Aplica value_maps (si existe)
      - Castea según df_schema (solo columnas presentes)
      - Normaliza strings (vectorizado)
      - Limpieza opcional (drop_na / drop_duplicates)
    """
    if df is None or df.empty:
        # DataFrame vacío con columnas/dtypes del schema
        out = pd.DataFrame({c: pd.Series(dtype=t) for c, t in schema.df_schema.dtypes.items()})
        return out, None

    # Columnas destino esperadas y dtypes esperados
    expected_cols = list(schema.df_schema.columns)
    dtype_map = schema.df_schema.dtypes.to_dict()

    # 1) Selección y rename (solo columnas que existan en input)
    # map_dict: {dest: source}
    present_pairs = [(dest, src) for dest, src in schema.map_dict.items() if src in df.columns]
    if not present_pairs:
        out = pd.DataFrame({c: pd.Series(dtype=dtype_map.get(c, "object")) for c in expected_cols})
        return out, None

    src_cols = [src for _, src in present_pairs]
    rename_map = {src: dest for dest, src in present_pairs}

    data = df.loc[:, src_cols].rename(columns=rename_map).copy()

    # 2) Asegurar TODAS las columnas esperadas (aunque falten)
    # (evita KeyError luego y mantiene forma consistente)
    for c in expected_cols:
        if c not in data.columns:
            data[c] = pd.NA

    data = data[expected_cols]

    # 3) Value maps (recodificación por columna destino)
    data = schema.apply_value_maps(data)

    # 4) Cast de tipos (solo donde aplique; errors ignore para robustez)
    # Nota: mantener esto "suave" es útil cuando BigQuery trae floats/strings mezclados.
    data = data.astype(dtype_map, errors="ignore")

    # 5) Normalización de strings (solo object/string)
    # Evita tocar columnas clave si no quieres (ej. sku) => puedes excluirlas aquí.
    if normalice:
        for col in data.columns:
            if col not in data.columns:
                continue
            # Normaliza solo si parece texto
            if pd.api.types.is_object_dtype(data[col]) or pd.api.types.is_string_dtype(data[col]):
                # Excluir claves típicas si prefieres 0 intervención
                if col in {"sku"}:
                    continue
                data[col] = _normalize_text_series(data[col])

    # 6) Limpieza opcional
    if drop_na:
        data = data.dropna()

    if drop_duplicates:
        data = data.drop_duplicates()

    data = data.reset_index(drop=True)
    return data, None


# ======================================================================================
# Schemas competidores
# ======================================================================================

class GenericCompetidorSchema(BaseSchema):
    """
    Fallback si un competidor nuevo no tiene schema dedicado.
    """
    def __init__(self) -> None:
        super().__init__()
        self.df_schema = pd.DataFrame({
            "sku": pd.Series(dtype="Int64"),
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
        # Si Central usa exactamente estas columnas, no necesitas cambiar nada.
        self.map_dict = {
            "sku": "product_ean",
            "fecha": "extraction_date",
            "precio_lleno": "product_price",
            "precio_descuento": "product_discount_price",
        }
        # Ejemplo de recodificación (opcional):
        # self.value_maps = {
        #     "categoria": {"BEBIDAS_GASEOSAS": "BEBIDAS GASEOSAS"}
        # }


class AlviSchema(GenericCompetidorSchema):
    def __init__(self) -> None:
        super().__init__()
        self.map_dict = {
            "sku": "product_ean",
            "fecha": "extraction_date",
            "precio_lleno": "product_price",
            "precio_descuento": "product_min_price",
        }
        self.value_maps = {}


class AdelcoSchema(GenericCompetidorSchema):
    def __init__(self) -> None:
        super().__init__()
        self.map_dict = {
            "sku": "product_ean",
            "fecha": "extraction_date",
            "precio_lleno": "product_price",
            "precio_descuento": "product_discount_price",
        }
        self.value_maps = {}


class LaOfertaSchema(GenericCompetidorSchema):
    def __init__(self) -> None:
        super().__init__()
        self.map_dict = {
            "sku": "product_ean",
            "fecha": "extraction_date",
            "precio_lleno": "product_price",
            "precio_descuento": "product_discount_price",
        }
        self.value_maps = {}
