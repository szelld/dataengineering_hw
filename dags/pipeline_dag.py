import os
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context

from src.extract import check_market_api, extract_market_data, extract_news_data
from src.load import load_to_postgres
from src.transform import build_daily_dataset


@dag(
    dag_id="financial_market_sentiment_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["finance", "etl"],
)
def financial_market_sentiment_pipeline():
    @task(task_id="task_check_api")
    def task_check_api() -> bool:
        ticker = os.getenv("STOCK_TICKER", "AAPL")
        api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        ok = check_market_api(ticker=ticker, alpha_vantage_api_key=api_key)
        if not ok:
            raise AirflowFailException("No market API source is reachable")
        return True

    @task(task_id="task_extract")
    def task_extract() -> dict:
        context = get_current_context()
        execution_date = context["ds"]
        ticker = os.getenv("STOCK_TICKER", "AAPL")
        api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        source_news_csv = os.getenv("NEWS_SOURCE_CSV", "/opt/airflow/data/raw/news_feed.csv")

        market_path = extract_market_data(
            execution_date=execution_date,
            ticker=ticker,
            alpha_vantage_api_key=api_key,
        )
        news_path = extract_news_data(
            execution_date=execution_date,
            source_csv_path=source_news_csv,
            ticker=ticker,
        )

        return {
            "execution_date": execution_date,
            "ticker": ticker,
            "market_path": market_path,
            "news_path": news_path,
        }

    @task(task_id="task_transform")
    def task_transform(payload: dict) -> str:
        return build_daily_dataset(
            market_path=payload["market_path"],
            news_path=payload["news_path"],
            ticker=payload["ticker"],
            execution_date=payload["execution_date"],
        )

    @task(task_id="task_load")
    def task_load(processed_path: str) -> str:
        return load_to_postgres(processed_file_path=processed_path)

    api_ok = task_check_api()
    extracted = task_extract()
    transformed = task_transform(extracted)
    loaded = task_load(transformed)

    api_ok >> extracted >> transformed >> loaded


financial_market_sentiment_pipeline()
