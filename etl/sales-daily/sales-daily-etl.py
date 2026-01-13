import os
import json
import uuid
import time
import traceback
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

from utils.mySQLHelper import my_default_bulk_loader, execute_mysql_query
from dotenv import load_dotenv
load_dotenv()  # carga .env en os.environ


# =========================================================
# Logger de archivo: 1 línea = 1 evento JSON (fácil para Streamlit)
# =========================================================
def build_json_logger(log_path: str, logger_name: str = "sales-daily-etl") -> logging.Logger:
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

    # Formato: el mensaje ya es JSON; lo escribimos tal cual
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    return logger


def log_event(logger: logging.Logger, level: str, event: str, **fields) -> None:
    payload = {
        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "level": level,
        "event": event,
        **fields,
    }
    msg = json.dumps(payload, ensure_ascii=False)
    getattr(logger, level.lower())(msg)


# =========================================================
# ETL minimalista
# =========================================================
class SalesDailyETL:
    def __init__(self, spreadsheet_id: str, range_name: str, sa_json_path: str, log_path: str):
        self.spreadsheet_id = spreadsheet_id
        self.range_name = range_name
        self.sa_json_path = sa_json_path

        self.loader = my_default_bulk_loader()
        self.logger = build_json_logger(log_path)

        # Identificador por corrida (para agrupar en Streamlit)
        self.run_id = str(uuid.uuid4())

    # -------------------------
    # 1) Extract
    # -------------------------
    def extract(self) -> pd.DataFrame:
        creds = service_account.Credentials.from_service_account_file(
            self.sa_json_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
        )
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)

        res = service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=self.range_name,
            valueRenderOption="UNFORMATTED_VALUE",
            dateTimeRenderOption="FORMATTED_STRING",
        ).execute()

        values = res.get("values", [])
        if not values or len(values) < 2:
            return pd.DataFrame()

        headers = values[0]
        rows = values[1:]
        return pd.DataFrame(rows, columns=headers).dropna(how="all")

    # -------------------------
    # 2) Transform (solo último día)
    # -------------------------
    def transform_latest_day(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, str]:
        rename = {
            "SKU": "sku",
            "Name SKU": "nombre",
            "Provider ID": "id_proveedor",
            "Marketplace Category ID": "id_categoria",
            "Order Delivered At Date": "fecha",
            "SUM of Quantity": "cantidad",
            "Product Sales": "venta_neta",
            "Net Revenue": "ganancia_neta",
            "Total Discounted Value At Dynamic": "descuento_neto",
            "Last IVA (%)": "iva",
            "Product Front Gross Margin (%)": "front_gm",
            "Product Back Gross Margin (%)": "back",
        }
        missing = [c for c in rename.keys() if c not in df.columns]
        if missing:
            raise ValueError(f"Faltan columnas en Google Sheet: {missing}")

        df = df.rename(columns=rename).copy()

        df["sku"] = pd.to_numeric(df["sku"], errors="coerce")
        df["id_proveedor"] = pd.to_numeric(df["id_proveedor"], errors="coerce")
        df["id_categoria"] = pd.to_numeric(df["id_categoria"], errors="coerce")
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.date

        for c in ["cantidad", "venta_neta", "descuento_neto", "iva", "front_gm", "back"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df = df.dropna(subset=["sku", "id_proveedor", "id_categoria", "fecha"])
        if df.empty:
            return pd.DataFrame(), pd.DataFrame(), ""

        latest = max(df["fecha"])
        df = df[df["fecha"] == latest].copy()
        fecha_str = str(latest)

        def pct_to_ratio(s: pd.Series) -> pd.Series:
            s = s.fillna(0.0)
            return s.apply(lambda x: x / 100.0 if x > 1.0 else x)

        df["iva"] = pct_to_ratio(df["iva"]).clip(0, 1)
        df["front_gm"] = pct_to_ratio(df["front_gm"]).clip(0, 1)
        df["back"] = pct_to_ratio(df["back"]).clip(0, 1)

        df["venta_neta"] = df["venta_neta"].fillna(0.0)
        df["ganancia_neta"] = df["ganancia_neta"].fillna(0.0)

        df_sku = df[["sku", "nombre", "id_categoria", "id_proveedor"]].drop_duplicates().copy()
        df_sales = df[["sku", "fecha", "cantidad", "venta_neta", "ganancia_neta", "descuento_neto", "iva", "back"]].copy()

        df_sku["sku"] = df_sku["sku"].astype(int)
        df_sales["sku"] = df_sales["sku"].astype(int)
        df_sales["cantidad"] = df_sales["cantidad"].fillna(0).astype(int)
        df_sales["fecha"] = df_sales["fecha"].astype(str)

        return df_sku, df_sales, fecha_str

    # -------------------------
    # 3) Load SKUs faltantes
    # -------------------------
    def load_skus_missing(self, df_sku: pd.DataFrame, batch_size: int = 250) -> int:
        if df_sku.empty:
            return 0

        df_exist = execute_mysql_query("SELECT sku FROM sku;")
        if df_exist is None or df_exist.empty:
            df_new = df_sku
        else:
            df_new = df_sku.merge(df_exist, on="sku", how="left", indicator=True)
            df_new = df_new[df_new["_merge"] == "left_only"].drop(columns=["_merge"])

        if df_new.empty:
            return 0

        self.loader.bulk_insert_df(
            table_name="sku",
            df=df_new[["sku", "nombre", "id_categoria", "id_proveedor"]],
            batch_size=batch_size
        )
        return int(len(df_new))

    # -------------------------
    # 4) Load ventas del día (idempotente: DELETE + INSERT)
    # -------------------------
    def load_sales_day(self, df_sales: pd.DataFrame, fecha_str: str, batch_size: int = 2000, idempotent: bool = True) -> tuple[int, int]:
        if df_sales.empty:
            return 0, 0

        df_dicc = execute_mysql_query("SELECT id, sku FROM sku;")
        if df_dicc is None or df_dicc.empty:
            raise RuntimeError("No se pudo obtener SELECT id, sku FROM sku;")

        df_sales = df_sales.merge(df_dicc, on="sku", how="left")
        df_sales.rename(columns={"id": "id_sku"}, inplace=True)
        df_sales = df_sales.dropna(subset=["id_sku"]).copy()
        df_sales["id_sku"] = df_sales["id_sku"].astype(int)

        deleted_approx = 0
        if idempotent:
            execute_mysql_query(f"DELETE FROM ventas_chiper WHERE fecha = '{fecha_str}';")
            # Para log/monitoring: aproximamos deleted como lo que vamos a insertar.
            deleted_approx = int(len(df_sales))

        df_carga = df_sales[["id_sku", "fecha", "cantidad", "venta_neta", "ganancia_neta", "descuento_neto", "iva", "back"]].copy()

        self.loader.bulk_insert_df(
            table_name="ventas_chiper",
            df=df_carga,
            batch_size=batch_size
        )
        inserted = int(len(df_carga))
        return deleted_approx, inserted

    # -------------------------
    # Run end-to-end
    # -------------------------
    def run(self) -> dict:
        t0 = time.time()
        log_event(
            self.logger, "INFO", "etl_start",
            run_id=self.run_id,
            spreadsheet_id=self.spreadsheet_id,
            range=self.range_name
        )

        try:
            df_raw = self.extract()
            log_event(self.logger, "INFO", "extract_done", run_id=self.run_id, rows=int(len(df_raw)))

            if df_raw.empty:
                log_event(self.logger, "WARNING", "empty_sheet", run_id=self.run_id, elapsed_s=round(time.time() - t0, 2))
                return {"status": "empty_sheet", "run_id": self.run_id}

            df_sku, df_sales, target_date = self.transform_latest_day(df_raw)
            log_event(
                self.logger, "INFO", "transform_done",
                run_id=self.run_id,
                target_date=target_date,
                sku_rows=int(len(df_sku)),
                sales_rows=int(len(df_sales))
            )

            if not target_date:
                log_event(self.logger, "WARNING", "no_valid_dates", run_id=self.run_id, elapsed_s=round(time.time() - t0, 2))
                return {"status": "no_valid_dates", "run_id": self.run_id}

            sku_new = self.load_skus_missing(df_sku, batch_size=250)
            log_event(self.logger, "INFO", "load_sku_done", run_id=self.run_id, sku_new_inserted=int(sku_new))

            deleted, inserted = self.load_sales_day(df_sales, target_date, batch_size=2000, idempotent=True)
            log_event(
                self.logger, "INFO", "load_sales_done",
                run_id=self.run_id,
                target_date=target_date,
                sales_deleted_approx=int(deleted),
                sales_inserted=int(inserted)
            )

            elapsed = round(time.time() - t0, 2)
            log_event(
                self.logger, "INFO", "etl_finish_ok",
                run_id=self.run_id,
                target_date=target_date,
                elapsed_s=elapsed
            )
            return {
                "status": "ok",
                "run_id": self.run_id,
                "target_date": target_date,
                "sku_new_inserted": sku_new,
                "sales_deleted_approx": deleted,
                "sales_inserted": inserted,
                "elapsed_s": elapsed
            }

        except Exception as e:
            tb = traceback.format_exc()
            log_event(
                self.logger, "ERROR", "etl_finish_error",
                run_id=self.run_id,
                error=str(e),
                traceback=tb
            )
            return {"status": "error", "run_id": self.run_id, "error": str(e)}


def main():
    env_path = ".env"  # scripts/ -> raíz
    load_dotenv(dotenv_path=env_path, override=False)
    spreadsheet_id = os.getenv("GS_SPREADSHEET_ID", "1JGEcYm_bBekbpluORXKwCgTw5LBhgN6xWHL8WPJnXf0")
    range_name     = os.getenv("GS_RANGE", "Base Pricing!A:P")
    sa_json_path   = os.getenv("GS_SA_JSON_PATH", "pricingdata-483617-beffcf8f55ac.json")

    # Importante: ruta ABSOLUTA en el VPS (recomendado)
    log_path       = os.getenv("ETL_LOG_PATH", "logs/sales_daily_etl.jsonl")

    etl = SalesDailyETL(
        spreadsheet_id=spreadsheet_id,
        range_name=range_name,
        sa_json_path=sa_json_path,
        log_path=log_path
    )
    print(etl.run())

if __name__ == "__main__":
    main()


'''
cd /srv/
git clone https://github.com/tastudillo-star/pricing-data-services.git
cd pricing-data-services

cd /srv/pricing-data-services
git pull

cd /srv/pricing-data-services

# 1) Crear venv
python3 -m venv .venv

# 2) Activarlo
source .venv/bin/activate

# 3) Actualizar pip e instalar requirements
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

deactivate

'''