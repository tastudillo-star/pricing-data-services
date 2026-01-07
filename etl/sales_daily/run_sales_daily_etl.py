import os

from dotenv import load_dotenv
load_dotenv()  # carga .env en os.environ
from sales_daily_etl import SalesDailyETL


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


'''