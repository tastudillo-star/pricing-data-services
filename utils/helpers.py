from csv import DictReader
import os
import unicodedata
import re
import pandas as pd
from pathlib import Path
from utils.schemas import BaseSchema
from typing import Any, Dict, Optional

## Helper para normalizar data
def normalize_name(s) -> str:
    """Quita tildes y caracteres no ASCII, pasa a minúsculas y convierte all a [a-z0-9_]."""
    if pd.isna(s):
        return ""
    s = str(s)
    s = unicodedata.normalize('NFKD', s)
    s = s.encode('ascii', 'ignore').decode('ascii')
    s = s.lower()
    s = re.sub(r'[^a-z0-9_]+', '_', s)
    s = re.sub(r'_+', '_', s).strip('_')
    return s

def load_and_normalice(ruta_csv: str, schema: BaseSchema, json: bool = False, normalice: bool = True, sep: str = ',',
                       drop_na: bool = True, drop_duplicates: bool = False
                       ) -> tuple[pd.DataFrame, Optional[list[Dict[str, Any]]]]:
    forma = schema.df_schema
    map_dict = schema.map_dict

    if not isinstance(ruta_csv, str):
        raise TypeError(f"Se esperaba una ruta de archivo (str), recibido {type(ruta_csv).__name__}")

    if not os.path.exists(ruta_csv):
        raise FileNotFoundError(f"No se encontró el archivo en la ruta especificada: {ruta_csv}")

    if os.path.isdir(ruta_csv):
        raise ValueError(f"La ruta proporcionada es un directorio, no un archivo CSV: {ruta_csv}")

    if not ruta_csv.lower().endswith(".csv"):
        raise ValueError(f"El archivo debe tener extensión '.csv'. Ruta recibida: {ruta_csv}")

    data = pd.read_csv(ruta_csv, sep=sep)
    data_copy = data.copy()
    new_df = forma.copy()
    ###############################################################
    original_cols = [v for v in map_dict.values() if v in data_copy.columns]
    rename_map = {v: k for k, v in map_dict.items() if v in original_cols}

    # 1. Seleccionar y renombrar columnas
    data_copy = data_copy[original_cols].rename(columns=rename_map)
    # 2. Convertir tipos para que coincidan con master_df
    data_copy = data_copy.astype(new_df.dtypes.to_dict())
    # 3. Normalizar todas las columnas
    if normalice:
        for col in data_copy.select_dtypes(include=['object']).columns:
            data_copy[col] = data_copy[col].apply(normalize_name)
    # 4. Insertar filas en master_df
    new_df = pd.concat([new_df, data_copy], ignore_index=True)
    # 5. dropear na
    if drop_na:
        new_df.dropna(inplace=True)
    # 6. Eliminar duplicados
    if drop_duplicates:
        new_df.drop_duplicates(inplace=True)
    # 7. Resetear índice
    new_df.reset_index(drop=True, inplace=True)
    # 8. Liberar memoria
    try:
        del data_copy
    except NameError:
        pass

    if json:
        json_list = schema.to_json_list(new_df)
        return new_df, json_list
    else:
        return new_df, None

def normalize_dataframe(
    df: pd.DataFrame,
    schema: BaseSchema,
    normalice: bool = True,
    drop_na: bool = True,
    drop_duplicates: bool = False,
) -> tuple[pd.DataFrame, Optional[list[Dict[str, Any]]]]:
    """
    Normaliza un DataFrame ya cargado en memoria según un Schema.
    NO hace I/O. Pensado para ETLs BigQuery / API / memoria.
    """
    forma = schema.df_schema
    map_dict = schema.map_dict

    data_copy = df.copy()
    new_df = forma.copy()

    # 1. Seleccionar y renombrar columnas
    original_cols = [v for v in map_dict.values() if v in data_copy.columns]
    rename_map = {v: k for k, v in map_dict.items() if v in original_cols}

    data_copy = data_copy[original_cols].rename(columns=rename_map)

    # 2. Cast de tipos
    data_copy = data_copy.astype(new_df.dtypes.to_dict(), errors="ignore")

    # 3. Normalizar strings
    if normalice:
        for col in data_copy.select_dtypes(include=["object"]).columns:
            data_copy[col] = data_copy[col].apply(normalize_name)

    # 4. Merge con schema base
    new_df = pd.concat([new_df, data_copy], ignore_index=True)

    # 5. Limpieza
    if drop_na:
        new_df.dropna(inplace=True)

    if drop_duplicates:
        new_df.drop_duplicates(inplace=True)

    new_df.reset_index(drop=True, inplace=True)

    return new_df, None



def ruta_dicts(d: str):
    RUTA_DICTS = lambda d: (Path.cwd().parents[0] / 'Data' / 'Diccionarios' / d).as_posix()
    return RUTA_DICTS(d)