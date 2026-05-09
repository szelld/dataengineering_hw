import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.python import get_current_context

from src.extract import extract_market_data, extract_eonet_data
from src.load import load_to_postgres
from src.transform import build_daily_datasets_cached


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
    """Get tickers from DAG config, Airflow variable, or environment (default: disaster-sensitive industries)."""
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

    # Default: insurers/reinsurers exposed to wildfire/storm claim risk.
    return _parse_tickers(os.getenv("STOCK_TICKER", "ALL,CB,TRV,AIG,PGR,BRK-B"))


def _get_target_execution_date(context: dict) -> str:
    ds = context["ds"]
    dag_run = context.get("dag_run")

    lookback_days = int(os.getenv("LOOKBACK_DAYS", "30"))
    if dag_run and dag_run.conf and dag_run.conf.get("lookback_days") is not None:
        lookback_days = int(dag_run.conf["lookback_days"])

    run_date = datetime.strptime(ds, "%Y-%m-%d")
    target_date = run_date - timedelta(days=lookback_days)
    return target_date.strftime("%Y-%m-%d")


def _get_analysis_window(context: dict) -> tuple[str, str]:
    """Return the inclusive date window processed by a single DAG run."""
    dag_run = context.get("dag_run")
    default_start = os.getenv("ANALYSIS_START_DATE", "2024-12-01")
    default_end = os.getenv("ANALYSIS_END_DATE", "2025-03-31")

    if dag_run and dag_run.conf:
        start_date = dag_run.conf.get("analysis_start_date", default_start)
        end_date = dag_run.conf.get("analysis_end_date", default_end)
    else:
        start_date = default_start
        end_date = default_end

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    if start_dt > end_dt:
        raise AirflowFailException("analysis_start_date must be before or equal to analysis_end_date")

    return start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")


def _get_analysis_window_adaptive(context: dict) -> tuple[str, str]:
    """Return the analysis window for this run.

    - **Backfill mode**: when the DAG run date is on or before ANALYSIS_END_DATE
      the full configured historical window is returned so the pipeline processes
      the Dec 2024 – Mar 2025 block.
    - **Daily mode**: when the DAG run date is after ANALYSIS_END_DATE (i.e.
      routine scheduled runs) only yesterday is returned, keeping each daily
      execution lightweight.
    """
    ds = context["ds"]
    run_date = datetime.strptime(ds, "%Y-%m-%d")
    default_end = os.getenv("ANALYSIS_END_DATE", "2025-03-31")
    hist_end = datetime.strptime(default_end, "%Y-%m-%d")

    if run_date <= hist_end:
        # Backfill / historical mode — process the full configured window.
        return _get_analysis_window(context)

    # Daily mode — process only yesterday so each scheduled run is fast.
    yesterday = (run_date - timedelta(days=1)).strftime("%Y-%m-%d")
    return yesterday, yesterday


def _business_dates_between(start_date: str, end_date: str) -> list[str]:
    """Return weekdays in the inclusive analysis window."""
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    dates = []
    current_dt = start_dt
    while current_dt <= end_dt:
        if current_dt.weekday() < 5:
            dates.append(current_dt.strftime("%Y-%m-%d"))
        current_dt += timedelta(days=1)
    return dates


@dag(
    dag_id="disaster_stock_correlation_pipeline",
    start_date=datetime(2024, 12, 1),
    schedule="@daily",
    catchup=False,
    tags=["disaster", "etl", "nasa"],
)
def disaster_stock_correlation_pipeline():
    """
    NASA EONET Disaster Data + Stock Price Correlation Pipeline.
    
    Extracts natural disaster events from NASA EONET API and correlates
    disaster intensity (wildfires, severe storms) with stock performance
    of disaster-sensitive industries (Insurance, Construction, Energy).
    """

    @task(task_id="task_check_api")
    def task_check_api() -> bool:
        context = get_current_context()
        tickers = _get_tickers(context)
        start_date, end_date = _get_analysis_window(context)
        if not tickers:
            raise AirflowFailException("No tickers configured")
        print(f"Pipeline configured for tickers: {', '.join(tickers)}")
        print(f"Analysis window: {start_date} to {end_date}")
        return True

    @task(task_id="task_extract_eonet")
    def task_extract_eonet() -> str:
        """Extract NASA EONET disaster data (shared across all tickers)."""
        context = get_current_context()
        analysis_start_date, execution_date = _get_analysis_window_adaptive(context)
        print(f"task_extract_eonet | start analysis_window={analysis_start_date}..{execution_date}")
        
        try:
            eonet_path = extract_eonet_data(
                execution_date=execution_date,
                start_date=analysis_start_date,
                end_date=execution_date,
            )
            print(f"task_extract_eonet | success eonet_path={eonet_path}")
            return eonet_path
        except Exception as exc:
            print(f"task_extract_eonet | failed error={exc}")
            raise AirflowFailException(f"EONET extraction failed: {exc}")

    @task(task_id="task_extract_stocks")
    def task_extract_stocks() -> list[dict]:
        """Extract stock market data for disaster-sensitive industries."""
        context = get_current_context()
        analysis_start_date, analysis_end_date = _get_analysis_window_adaptive(context)
        analysis_dates = _business_dates_between(analysis_start_date, analysis_end_date)
        tickers = _get_tickers(context)
        api_key = os.getenv("ALPHA_VANTAGE_API_KEY")

        payloads = []
        failures = []
        for ticker in tickers:
            print(
                "task_extract_stocks | start "
                f"ticker={ticker} analysis_window={analysis_start_date}..{analysis_end_date}"
            )
            try:
                market_path = extract_market_data(
                    execution_date=analysis_end_date,
                    ticker=ticker,
                    alpha_vantage_api_key=api_key,
                )
                for execution_date in analysis_dates:
                    payloads.append(
                        {
                            "execution_date": execution_date,
                            "ticker": ticker,
                            "market_path": market_path,
                        }
                    )
                print(
                    "task_extract_stocks | success "
                    f"ticker={ticker} market_path={market_path} dates={len(analysis_dates)}"
                )
            except Exception as exc:
                print(f"task_extract_stocks | failed ticker={ticker} error={exc}")
                failures.append(f"{ticker}: {exc}")

        if failures:
            print("Skipped tickers due to extract errors:")
            for failure in failures:
                print(f"- {failure}")

        if not payloads:
            raise AirflowFailException("Stock extraction failed for all tickers")

        return payloads

    @task(task_id="task_transform")
    def task_transform(payload: dict, eonet_path: str) -> dict:
        """Transform stock + disaster data into fact tables.

        Uses the filesystem cache: if ``fact_daily_impact_{ticker}_{date}.csv``
        already exists the heavy transform is skipped and the cached path is
        returned immediately.  Pass ``{"force_reprocess": true}`` in the DAG
        run configuration to bypass the cache (e.g. after changing transform
        logic).
        """
        context = get_current_context()
        dag_run = context.get("dag_run")
        force_reprocess = bool(
            dag_run and dag_run.conf and dag_run.conf.get("force_reprocess", False)
        )
        print(
            f"task_transform | ticker={payload['ticker']} "
            f"date={payload['execution_date']} "
            f"force_reprocess={force_reprocess} "
            f"eonet_path={eonet_path}"
        )
        return build_daily_datasets_cached(
            market_path=payload["market_path"],
            eonet_path=eonet_path,
            ticker=payload["ticker"],
            execution_date=payload["execution_date"],
            force_reprocess=force_reprocess,
        )

    @task(task_id="task_load")
    def task_load(processed_paths: dict) -> str:
        """Load transformed data into PostgreSQL data warehouse."""
        return load_to_postgres(processed_file_path=processed_paths["fact_path"])

    # DAG task dependencies
    api_ok = task_check_api()
    eonet_data = task_extract_eonet()
    stock_data = task_extract_stocks()
    
    # Use .partial() for shared eonet_path, .expand() for per-ticker payload
    transformed = task_transform.partial(eonet_path=eonet_data).expand(payload=stock_data)
    loaded = task_load.expand(processed_paths=transformed)

    api_ok >> [eonet_data, stock_data]
    [eonet_data, stock_data] >> transformed >> loaded


disaster_stock_correlation_pipeline()
