"""Provision the LA Wildfire Insurance Impact dashboard in Metabase.

This script is idempotent and self-contained:

1. If Metabase has not been set up yet, it performs first-run setup
   (creates the admin user from the configured credentials).
2. Ensures the ``disaster_dw`` PostgreSQL warehouse is connected and synced.
3. Creates (or updates) a set of native-SQL questions.
4. Assembles them into the "LA Wildfire Insurance Impact" dashboard.

Configuration is read from environment variables so no secrets are hardcoded:

    MB_BASE_URL   default http://metabase:3000
    MB_USERNAME   default test@gamil.com
    MB_PASSWORD   required
    PG_HOST/PG_PORT/PG_DB/PG_USER/PG_PASSWORD  warehouse connection details
"""

import os
import time

import requests


BASE_URL = os.getenv("MB_BASE_URL", "http://metabase:3000").rstrip("/")
USERNAME = os.getenv("MB_USERNAME", "test@gamil.com")
PASSWORD = os.environ["MB_PASSWORD"]

DASHBOARD_NAME = "LA Wildfire Insurance Impact"
WAREHOUSE_DISPLAY_NAME = "Disaster DW"

PG_DETAILS = {
    "host": os.getenv("PG_HOST", "postgres"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "dbname": os.getenv("PG_DB", "disaster_dw"),
    "user": os.getenv("PG_USER", "airflow"),
    "password": os.getenv("PG_PASSWORD", "airflow"),
    "ssl": False,
}

TIMEOUT = 60


def wait_for_metabase() -> None:
    """Block until the Metabase health endpoint reports ready."""
    deadline = time.time() + 300
    while time.time() < deadline:
        try:
            response = requests.get(f"{BASE_URL}/api/health", timeout=10)
            if response.ok and response.json().get("status") == "ok":
                return
        except requests.RequestException:
            pass
        time.sleep(5)
    raise RuntimeError("Metabase did not become healthy in time")


def get_session() -> dict[str, str]:
    """Return auth headers, performing first-run setup if required."""
    properties = requests.get(f"{BASE_URL}/api/session/properties", timeout=TIMEOUT).json()

    if not properties.get("has-user-setup"):
        setup_token = properties.get("setup-token")
        if not setup_token:
            raise RuntimeError("Metabase reports no user setup but exposes no setup-token")
        session_id = _run_first_time_setup(setup_token)
    else:
        response = requests.post(
            f"{BASE_URL}/api/session",
            json={"username": USERNAME, "password": PASSWORD},
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        session_id = response.json()["id"]

    return {"X-Metabase-Session": session_id}


def _run_first_time_setup(setup_token: str) -> str:
    payload = {
        "token": setup_token,
        "user": {
            "first_name": "Pipeline",
            "last_name": "Admin",
            "email": USERNAME,
            "password": PASSWORD,
            "site_name": "LA Wildfire Analytics",
        },
        "prefs": {"site_name": "LA Wildfire Analytics", "allow_tracking": False},
        "database": {
            "engine": "postgres",
            "name": WAREHOUSE_DISPLAY_NAME,
            "details": PG_DETAILS,
        },
    }
    response = requests.post(f"{BASE_URL}/api/setup", json=payload, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()["id"]


def _as_list(payload) -> list:
    if isinstance(payload, dict) and "data" in payload:
        return payload["data"]
    return payload


def ensure_database(headers: dict[str, str]) -> int:
    """Return the warehouse database id, creating the connection if missing."""
    response = requests.get(f"{BASE_URL}/api/database", headers=headers, timeout=TIMEOUT)
    response.raise_for_status()
    databases = _as_list(response.json())

    for database in databases:
        details = database.get("details") or {}
        if details.get("dbname") == PG_DETAILS["dbname"] or database.get("name") in (
            WAREHOUSE_DISPLAY_NAME,
            "Chatastrophy",
            "Disaster DW",
        ):
            return int(database["id"])

    response = requests.post(
        f"{BASE_URL}/api/database",
        headers=headers,
        json={"engine": "postgres", "name": WAREHOUSE_DISPLAY_NAME, "details": PG_DETAILS},
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return int(response.json()["id"])


def sync_database(headers: dict[str, str], database_id: int) -> None:
    requests.post(
        f"{BASE_URL}/api/database/{database_id}/sync_schema",
        headers=headers,
        timeout=TIMEOUT,
    )
    # Give Metabase a moment to register the warehouse tables/views.
    time.sleep(8)


def existing_cards_by_name(headers: dict[str, str]) -> dict[str, int]:
    response = requests.get(f"{BASE_URL}/api/card", headers=headers, timeout=TIMEOUT)
    response.raise_for_status()
    return {card["name"]: int(card["id"]) for card in _as_list(response.json())}


def create_or_update_card(headers: dict[str, str], database_id: int, card: dict, existing: dict[str, int]) -> int:
    body = {
        "name": card["name"],
        "description": card["description"],
        "display": card["display"],
        "dataset_query": {
            "type": "native",
            "native": {"query": card["sql"].strip()},
            "database": database_id,
        },
        "visualization_settings": card.get("settings", {}),
    }

    if card["name"] in existing:
        card_id = existing[card["name"]]
        response = requests.put(
            f"{BASE_URL}/api/card/{card_id}", headers=headers, json=body, timeout=TIMEOUT
        )
        response.raise_for_status()
        return card_id

    response = requests.post(f"{BASE_URL}/api/card", headers=headers, json=body, timeout=TIMEOUT)
    response.raise_for_status()
    return int(response.json()["id"])


def get_or_create_dashboard(headers: dict[str, str]) -> int:
    response = requests.get(f"{BASE_URL}/api/dashboard", headers=headers, timeout=TIMEOUT)
    response.raise_for_status()
    for dashboard in _as_list(response.json()):
        if dashboard.get("name") == DASHBOARD_NAME:
            return int(dashboard["id"])

    response = requests.post(
        f"{BASE_URL}/api/dashboard",
        headers=headers,
        json={
            "name": DASHBOARD_NAME,
            "description": "How insurer stock prices reacted to the January 2025 Los Angeles wildfires.",
        },
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return int(response.json()["id"])


def set_dashboard_cards(headers: dict[str, str], dashboard_id: int, dashcards: list[dict]) -> None:
    """Replace dashboard cards using the Metabase v0.51 ``dashcards`` payload."""
    response = requests.put(
        f"{BASE_URL}/api/dashboard/{dashboard_id}",
        headers=headers,
        json={"dashcards": dashcards},
        timeout=TIMEOUT,
    )
    response.raise_for_status()


def set_as_homepage(headers: dict[str, str], dashboard_id: int) -> None:
    """Pin the dashboard as the Metabase landing page so plots show on open."""
    for setting, value in (
        ("custom-homepage", True),
        ("custom-homepage-dashboard", dashboard_id),
    ):
        requests.put(
            f"{BASE_URL}/api/setting/{setting}",
            headers=headers,
            json={"value": value},
            timeout=TIMEOUT,
        )


def card_definitions() -> list[dict]:
    return [
        {
            "name": "Insurance Stock Closing Prices",
            "display": "line",
            "description": "Daily closing price per insurer across the analysis window.",
            "sql": """
SELECT date_key, ticker, stock_close_price
FROM fact_stock_daily
WHERE date_key BETWEEN '2024-12-01' AND '2025-02-28'
ORDER BY date_key, ticker;
""",
            "settings": {
                "graph.dimensions": ["date_key", "ticker"],
                "graph.metrics": ["stock_close_price"],
                "graph.x_axis.title_text": "Date",
                "graph.y_axis.title_text": "Close price (USD)",
            },
            "layout": {"row": 0, "col": 0, "size_x": 12, "size_y": 7},
        },
        {
            "name": "LA Nearby Disaster Count Over Time",
            "display": "line",
            "description": "Daily count of natural-disaster events within the impact radius of Los Angeles.",
            "sql": """
SELECT date_key, nearby_disaster_count
FROM fact_city_disaster_daily
WHERE city_id = 'US-LOS_ANGELES'
  AND date_key BETWEEN '2024-12-01' AND '2025-02-28'
ORDER BY date_key;
""",
            "settings": {
                "graph.dimensions": ["date_key"],
                "graph.metrics": ["nearby_disaster_count"],
                "graph.x_axis.title_text": "Date",
                "graph.y_axis.title_text": "Nearby disasters",
            },
            "layout": {"row": 0, "col": 12, "size_x": 12, "size_y": 7},
        },
        {
            "name": "Insurance Daily % Price Change (LA)",
            "display": "line",
            "description": "Day-over-day percentage change in close price for insurers, joined to the Los Angeles risk row.",
            "sql": """
SELECT date_key, ticker, pct_close_change
FROM vw_stock_disaster_price_movement
WHERE city_id = 'US-LOS_ANGELES'
  AND pct_close_change IS NOT NULL
  AND date_key BETWEEN '2024-12-01' AND '2025-02-28'
ORDER BY date_key, ticker;
""",
            "settings": {
                "graph.dimensions": ["date_key", "ticker"],
                "graph.metrics": ["pct_close_change"],
                "graph.x_axis.title_text": "Date",
                "graph.y_axis.title_text": "% change",
            },
            "layout": {"row": 7, "col": 0, "size_x": 12, "size_y": 7},
        },
        {
            "name": "Total Nearby Disasters by City",
            "display": "bar",
            "description": "Total nearby disaster events per tracked city over the analysis window.",
            "sql": """
SELECT c.city_name, SUM(f.nearby_disaster_count) AS total_nearby_disasters
FROM fact_city_disaster_daily f
JOIN dim_city c ON c.city_id = f.city_id
WHERE f.date_key BETWEEN '2024-12-01' AND '2025-02-28'
GROUP BY c.city_name
ORDER BY total_nearby_disasters DESC;
""",
            "settings": {
                "graph.dimensions": ["city_name"],
                "graph.metrics": ["total_nearby_disasters"],
                "graph.x_axis.title_text": "City",
                "graph.y_axis.title_text": "Nearby disasters",
            },
            "layout": {"row": 7, "col": 12, "size_x": 12, "size_y": 7},
        },
        {
            "name": "Peak LA Nearby Disasters",
            "display": "scalar",
            "description": "Maximum single-day nearby disaster count recorded for Los Angeles.",
            "sql": """
SELECT MAX(nearby_disaster_count) AS peak_nearby_disasters
FROM fact_city_disaster_daily
WHERE city_id = 'US-LOS_ANGELES'
  AND date_key BETWEEN '2024-12-01' AND '2025-02-28';
""",
            "settings": {},
            "layout": {"row": 14, "col": 0, "size_x": 6, "size_y": 4},
        },
        {
            "name": "LA Disaster Risk Days Detail",
            "display": "table",
            "description": "Insurer rows on days when Los Angeles had nearby natural-disaster exposure.",
            "sql": """
SELECT date_key, ticker, company_name, stock_close_price,
       active_disaster_count, nearby_disaster_count,
       city_name, nearest_disaster_distance_km
FROM vw_la_disaster_insurance_risk_days
WHERE date_key BETWEEN '2024-12-01' AND '2025-02-28'
ORDER BY date_key, ticker;
""",
            "settings": {},
            "layout": {"row": 14, "col": 6, "size_x": 18, "size_y": 8},
        },
        # --- Rolling "Last 3 Months" section -------------------------------
        # Mirrors the LA-wildfire plots above but on a dynamic trailing
        # 3-month window (no backfill needed; empty where there is no data).
        {
            "name": "Latest Pipeline Data Date",
            "display": "scalar",
            "description": "Most recent calendar day loaded by the pipeline (updated by each daily/scheduled run).",
            "sql": """
SELECT MAX(date_key) AS latest_loaded_date
FROM fact_city_disaster_daily;
""",
            "settings": {},
            "layout": {"row": 22, "col": 0, "size_x": 6, "size_y": 4},
        },
        {
            "name": "Peak LA Nearby Disasters (Last 3 Months)",
            "display": "scalar",
            "description": "Maximum single-day nearby disaster count for Los Angeles in the trailing 3 months.",
            "sql": """
SELECT MAX(nearby_disaster_count) AS peak_nearby_disasters
FROM fact_city_disaster_daily
WHERE city_id = 'US-LOS_ANGELES'
  AND date_key >= (CURRENT_DATE - INTERVAL '3 months');
""",
            "settings": {},
            "layout": {"row": 22, "col": 6, "size_x": 6, "size_y": 4},
        },
        {
            "name": "Insurance Stock Closing Prices (Last 3 Months)",
            "display": "line",
            "description": "Daily closing price per insurer over the trailing 3 months.",
            "sql": """
SELECT date_key, ticker, stock_close_price
FROM fact_stock_daily
WHERE date_key >= (CURRENT_DATE - INTERVAL '3 months')
ORDER BY date_key, ticker;
""",
            "settings": {
                "graph.dimensions": ["date_key", "ticker"],
                "graph.metrics": ["stock_close_price"],
                "graph.x_axis.title_text": "Date",
                "graph.y_axis.title_text": "Close price (USD)",
            },
            "layout": {"row": 26, "col": 0, "size_x": 12, "size_y": 7},
        },
        {
            "name": "LA Nearby Disaster Count Over Time (Last 3 Months)",
            "display": "line",
            "description": "Daily count of natural-disaster events within the impact radius of Los Angeles over the trailing 3 months.",
            "sql": """
SELECT date_key, nearby_disaster_count
FROM fact_city_disaster_daily
WHERE city_id = 'US-LOS_ANGELES'
  AND date_key >= (CURRENT_DATE - INTERVAL '3 months')
ORDER BY date_key;
""",
            "settings": {
                "graph.dimensions": ["date_key"],
                "graph.metrics": ["nearby_disaster_count"],
                "graph.x_axis.title_text": "Date",
                "graph.y_axis.title_text": "Nearby disasters",
            },
            "layout": {"row": 26, "col": 12, "size_x": 12, "size_y": 7},
        },
        {
            "name": "Insurance Daily % Price Change (LA) (Last 3 Months)",
            "display": "line",
            "description": "Day-over-day percentage change in close price for insurers (LA risk row) over the trailing 3 months.",
            "sql": """
SELECT date_key, ticker, pct_close_change
FROM vw_stock_disaster_price_movement
WHERE city_id = 'US-LOS_ANGELES'
  AND pct_close_change IS NOT NULL
  AND date_key >= (CURRENT_DATE - INTERVAL '3 months')
ORDER BY date_key, ticker;
""",
            "settings": {
                "graph.dimensions": ["date_key", "ticker"],
                "graph.metrics": ["pct_close_change"],
                "graph.x_axis.title_text": "Date",
                "graph.y_axis.title_text": "% change",
            },
            "layout": {"row": 33, "col": 0, "size_x": 12, "size_y": 7},
        },
        {
            "name": "Total Nearby Disasters by City (Last 3 Months)",
            "display": "bar",
            "description": "Total nearby disaster events per tracked city over the trailing 3 months.",
            "sql": """
SELECT c.city_name, SUM(f.nearby_disaster_count) AS total_nearby_disasters
FROM fact_city_disaster_daily f
JOIN dim_city c ON c.city_id = f.city_id
WHERE f.date_key >= (CURRENT_DATE - INTERVAL '3 months')
GROUP BY c.city_name
ORDER BY total_nearby_disasters DESC;
""",
            "settings": {
                "graph.dimensions": ["city_name"],
                "graph.metrics": ["total_nearby_disasters"],
                "graph.x_axis.title_text": "City",
                "graph.y_axis.title_text": "Nearby disasters",
            },
            "layout": {"row": 33, "col": 12, "size_x": 12, "size_y": 7},
        },
        {
            "name": "LA Disaster Risk Days Detail (Last 3 Months)",
            "display": "table",
            "description": "Insurer rows on days when Los Angeles had nearby disaster exposure in the trailing 3 months.",
            "sql": """
SELECT date_key, ticker, company_name, stock_close_price,
       active_disaster_count, nearby_disaster_count,
       city_name, nearest_disaster_distance_km
FROM vw_la_disaster_insurance_risk_days
WHERE date_key >= (CURRENT_DATE - INTERVAL '3 months')
ORDER BY date_key, ticker;
""",
            "settings": {},
            "layout": {"row": 40, "col": 0, "size_x": 24, "size_y": 8},
        },
    ]


def main() -> None:
    wait_for_metabase()
    headers = get_session()
    database_id = ensure_database(headers)
    sync_database(headers, database_id)

    existing = existing_cards_by_name(headers)
    dashboard_id = get_or_create_dashboard(headers)

    dashcards = []
    for index, card in enumerate(card_definitions()):
        card_id = create_or_update_card(headers, database_id, card, existing)
        layout = card["layout"]
        dashcards.append(
            {
                "id": -(index + 1),
                "card_id": card_id,
                "row": layout["row"],
                "col": layout["col"],
                "size_x": layout["size_x"],
                "size_y": layout["size_y"],
                "series": [],
                "parameter_mappings": [],
                "visualization_settings": {},
            }
        )

    set_dashboard_cards(headers, dashboard_id, dashcards)
    set_as_homepage(headers, dashboard_id)

    print(
        {
            "dashboard": DASHBOARD_NAME,
            "dashboard_id": dashboard_id,
            "database_id": database_id,
            "cards": len(dashcards),
        }
    )

    public_url = os.getenv("MB_PUBLIC_URL", "http://localhost:3000").rstrip("/")
    dashboard_url = f"{public_url}/dashboard/{dashboard_id}"
    print("\n" + "=" * 60)
    print("Metabase is ready. Log in with:")
    print(f"  URL:       {public_url}")
    print(f"  Username:  {USERNAME}")
    print(f"  Password:  {PASSWORD}")
    print(f"  Dashboard: {dashboard_url}")
    print("=" * 60)


if __name__ == "__main__":
    main()
