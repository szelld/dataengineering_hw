import os
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.python import get_current_context

from src.extract import extract_market_data, extract_news_data
from src.load import load_to_postgres
from src.transform import build_daily_datasets


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    seen = set()
    unique_items = []
    for item in items:
        if item not in seen:
            seen.add(item)
            unique_items.append(item)
    return unique_items


def _parse_tickers(raw: str | list[str] | None) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        tickers = [str(item).strip().upper() for item in raw if str(item).strip()]
        return _dedupe_preserve_order(tickers)

    tickers = [ticker.strip().upper() for ticker in str(raw).split(",") if ticker.strip()]
    return _dedupe_preserve_order(tickers)


def _get_tickers(context: dict) -> list[str]:
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf:
        run_conf_tickers = dag_run.conf.get("tickers")
        parsed = _parse_tickers(run_conf_tickers)
        if parsed:
            return parsed

    variable_tickers = Variable.get("stock_tickers", default_var="").strip()
    parsed = _parse_tickers(variable_tickers)
    if parsed:
        return parsed

    tickers_env = os.getenv("STOCK_TICKERS", "").strip()
    parsed = _parse_tickers(tickers_env)
    if parsed:
        return parsed

    return _parse_tickers(os.getenv("STOCK_TICKER", "AAPL"))


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
        # Avoid aggressive pre-flight API probing, because it can trigger rate limits.
        context = get_current_context()
        tickers = _get_tickers(context)
        if not tickers:
            raise AirflowFailException("No tickers configured")
        return True

    @task(task_id="task_extract")
    def task_extract() -> list[dict]:
        context = get_current_context()
        execution_date = context["ds"]
        tickers = _get_tickers(context)
        api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        source_news_csv = os.getenv("NEWS_SOURCE_CSV", "/opt/airflow/data/raw/news_feed.csv")

        payloads = []
        failures = []
        for ticker in tickers:
            print(f"task_extract | start ticker={ticker} execution_date={execution_date}")
            try:
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
                payloads.append(
                    {
                        "execution_date": execution_date,
                        "ticker": ticker,
                        "market_path": market_path,
                        "news_path": news_path,
                    }
                )
                print(f"task_extract | success ticker={ticker} market_path={market_path} news_path={news_path}")
            except Exception as exc:
                print(f"task_extract | failed ticker={ticker} error={exc}")
                failures.append(f"{ticker}: {exc}")

        if failures:
            print("Skipped tickers due to extract errors:")
            for failure in failures:
                print(f"- {failure}")

        if not payloads:
            raise AirflowFailException("Extraction failed for all tickers")

        return payloads

    @task(task_id="task_transform")
    def task_transform(payload: dict) -> dict:
        return build_daily_datasets(
            market_path=payload["market_path"],
            news_path=payload["news_path"],
            ticker=payload["ticker"],
            execution_date=payload["execution_date"],
        )

    @task(task_id="task_load")
    def task_load(processed_paths: dict) -> str:
        return load_to_postgres(
            processed_file_path=processed_paths["fact_path"],
            news_sentiment_file_path=processed_paths["news_sentiment_path"],
        )

    api_ok = task_check_api()
    extracted = task_extract()
    transformed = task_transform.expand(payload=extracted)
    loaded = task_load.expand(processed_paths=transformed)

    api_ok >> extracted >> transformed >> loaded


financial_market_sentiment_pipeline()
