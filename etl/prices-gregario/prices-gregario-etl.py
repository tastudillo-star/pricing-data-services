from __future__ import annotations

import os
import json
import uuid
import time
import traceback
import logging
import datetime as dt
from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd
import pytz
from dotenv import load_dotenv
from google.oauth2 import service_account
from pandas_gbq import read_gbq
from logging.handlers import RotatingFileHandler
from utils.helpers import normalize_dataframe


# =========================================================
# Logger JSONL: 1 línea = 1 evento JSON (fácil para Streamlit)
# =========================================================
def build_json_logger(log_path: str, logger_name: str = "prices-gregario-etl") -> logging.Logger:
    """
    Loggea eventos como JSON por línea (JSONL):
      {"ts": "...", "level": "INFO", "event": "...", ...}
    Rotación: 5MB, 10 backups.
    """
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    logger = logging.getLogger(logger_name)
    if logger.handlers:
        return logger

    logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
    logger.propagate = False

    handler = RotatingFileHandler(
        log_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=10,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)

    return logger


def log_event(logger: logging.Logger, level: str, event: str, **kwargs) -> None:
    payload = {
        # UTC aware (evita warning)
        "ts": datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "level": level.upper(),
        "event": event,
        **kwargs,
    }
    msg = json.dumps(payload, ensure_ascii=False)
    getattr(logger, level.lower(), logger.info)(msg)


# ======================================================
# HELPERS
# ======================================================
def _env(key: str) -> str:
    v = os.getenv(key)
    if not v:
        raise RuntimeError(f"Falta variable de entorno: {key}")
    return v


def _target_date_iso() -> str:
    """
    Siempre 'ayer' en zona horaria local.
    Si corre el día 8 a las 05:00 (America/Santiago) => carga día 7.
    """
    tz = pytz.timezone(os.getenv("DEFAULT_TZ", "America/Santiago"))
    now = dt.datetime.now(tz)
    target = (now - dt.timedelta(days=1)).date()
    return target.isoformat()


def _table_map() -> Dict[str, str]:
    return {
        # activa/desactiva competidores aquí si quieres
        # "adelco": _env("BQ_ADELCO"),
        # "alvi": _env("BQ_ALVI"),
        "central_mayorista": _env("BQ_CENTRAL_MAYORISTA"),
        # "la_oferta": _env("BQ_LA_OFERTA"),
    }


# ======================================================
# CORE ETL
# ======================================================
def run_prices_gregario_etl() -> None:
    base_path = Path(__file__).parent

    # IMPORTANTÍSIMO: cargar .env ANTES de usar mySQLHelper
    load_dotenv(base_path / ".env", override=True)

    # Logger JSONL en logs/
    run_id = uuid.uuid4().hex
    # log_path = (base_path / "logs" / "prices_gregario_etl.jsonl").as_posix()
    repo_root = Path(__file__).resolve().parents[2]  # .../pricing-data-services
    log_path = (repo_root / "logs" / "prices_gregario_etl.jsonl").as_posix()

    logger = build_json_logger(log_path=log_path, logger_name="prices_gregario_etl")

    t0 = time.time()
    log_event(logger, "info", "etl_start", run_id=run_id, etl="prices-gregario")

    try:
        # Imports diferidos: aseguran que mySQLHelper vea las env vars ya cargadas
        from utils.helpers import load_and_normalice
        from utils.mySQLHelper import execute_mysql_query, my_default_bulk_loader
        from utils.schemas import CompetidorSchema

        project_id = _env("GCP_PROJECT_ID")
        cred_path = base_path / _env("GCP_CREDENTIALS_JSON")
        target_table = _env("MYSQL_TARGET_TABLE")
        fecha_objetivo = _target_date_iso()

        log_event(
            logger,
            "info",
            "config_loaded",
            run_id=run_id,
            project_id=project_id,
            target_table=target_table,
            fecha_objetivo=fecha_objetivo,
            tz=os.getenv("DEFAULT_TZ", "America/Santiago"),
            mysql_host=os.getenv("MYSQL_HOST"),
            mysql_user=os.getenv("MYSQL_USER"),
            mysql_db=os.getenv("MYSQL_DATABASE"),
            mysql_port=os.getenv("MYSQL_PORT"),
        )

        credentials = service_account.Credentials.from_service_account_file(
            cred_path.as_posix()
        )

        # Diccionarios MySQL (una sola vez)
        df_sku = execute_mysql_query("SELECT id, sku FROM sku;")
        df_comp = execute_mysql_query("SELECT id, nombre FROM competidor;")

        if df_sku is None or df_comp is None:
            raise RuntimeError(
                "Falló carga de diccionarios MySQL (None). "
                "Causa típica: mySQLHelper inicializó antes del load_dotenv."
            )

        log_event(
            logger,
            "info",
            "mysql_dictionaries_loaded",
            run_id=run_id,
            sku_rows=int(len(df_sku)),
            comp_rows=int(len(df_comp)),
        )

        loader = my_default_bulk_loader()

        for competidor, table in _table_map().items():
            step_t0 = time.time()
            log_event(
                logger,
                "info",
                "competitor_start",
                run_id=run_id,
                competidor=competidor,
                bq_table=table,
            )

            print(fecha_objetivo)

            query = f"""
            SELECT *
            FROM `{table}`
            WHERE DATE(extraction_date) = '{fecha_objetivo}'
            """

            df_bq = read_gbq(
                query,
                project_id=project_id,
                credentials=credentials,
            )

            if df_bq.empty:
                log_event(
                    logger,
                    "warning",
                    "bq_empty",
                    run_id=run_id,
                    competidor=competidor,
                    fecha_objetivo=fecha_objetivo,
                    elapsed_s=round(time.time() - step_t0, 3),
                )
                continue

            log_event(
                logger,
                "info",
                "bq_downloaded",
                run_id=run_id,
                competidor=competidor,
                rows=int(len(df_bq)),
                elapsed_s=round(time.time() - step_t0, 3),
            )

            # Normalización


            df_norm, _ = normalize_dataframe(
                df_bq,
                CompetidorSchema(),
                drop_duplicates=False,
                drop_na=False,
            )

            before_dedup = int(len(df_norm))
            df_norm.drop_duplicates(subset=["sku", "fecha"], inplace=True)
            after_dedup = int(len(df_norm))
            df_norm["competidor"] = competidor

            # --- FIX TIPO SKU (clave de join) ---
            df_norm["sku"] = df_norm["sku"].astype(str)
            df_sku["sku"] = df_sku["sku"].astype(str)

            # Join SKU
            df = df_norm.merge(df_sku, on="sku", how="inner")
            df.rename(columns={"id": "id_sku"}, inplace=True)

            # Join Competidor
            df = df.merge(
                df_comp,
                left_on="competidor",
                right_on="nombre",
                how="left",
            )
            df.rename(columns={"id": "id_competidor"}, inplace=True)

            df = df[df["id_competidor"].notna()]

            if df.empty:
                log_event(
                    logger,
                    "warning",
                    "mysql_no_match",
                    run_id=run_id,
                    competidor=competidor,
                    before_dedup=before_dedup,
                    after_dedup=after_dedup,
                    elapsed_s=round(time.time() - step_t0, 3),
                )
                continue

            required_cols = ["id_competidor", "id_sku", "fecha", "precio_lleno", "precio_descuento"]
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                raise ValueError(f"Faltan columnas para carga MySQL: {missing}")

            df_carga = df[required_cols].copy()

            loader.bulk_insert_df(
                table_name=target_table,
                df=df_carga,
            )

            log_event(
                logger,
                "info",
                "competitor_loaded",
                run_id=run_id,
                competidor=competidor,
                bq_rows=int(len(df_bq)),
                norm_rows=int(after_dedup),
                carga_rows=int(len(df_carga)),
                elapsed_s=round(time.time() - step_t0, 3),
            )

        log_event(
            logger,
            "info",
            "etl_success",
            run_id=run_id,
            elapsed_s=round(time.time() - t0, 3),
        )

    except Exception as e:
        log_event(
            logger,
            "error",
            "etl_failed",
            run_id=run_id,
            error=str(e),
            traceback=traceback.format_exc(),
            elapsed_s=round(time.time() - t0, 3),
        )
        raise


if __name__ == "__main__":
    run_prices_gregario_etl()
