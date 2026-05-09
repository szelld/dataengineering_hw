import json
import logging
import math
import os
from pathlib import Path

import pandas as pd


PROCESSED_DIR = Path(os.getenv("PROCESSED_DIR", "/opt/airflow/data/processed"))
CITY_REFERENCE_PATH = Path(os.getenv("CITY_REFERENCE_PATH", "/opt/airflow/data/reference/insured_cities.csv"))
CITY_IMPACT_RADIUS_KM = float(os.getenv("CITY_IMPACT_RADIUS_KM", "100"))
LOGGER = logging.getLogger(__name__)

CITY_COLUMNS = ["city_id", "city_name", "country", "latitude", "longitude"]
DEFAULT_CITY_ROWS = [
    {"city_id": "US-LOS_ANGELES", "city_name": "Los Angeles", "country": "United States", "latitude": 34.0522, "longitude": -118.2437},
    {"city_id": "US-SAN_FRANCISCO", "city_name": "San Francisco", "country": "United States", "latitude": 37.7749, "longitude": -122.4194},
    {"city_id": "US-SACRAMENTO", "city_name": "Sacramento", "country": "United States", "latitude": 38.5816, "longitude": -121.4944},
    {"city_id": "US-DENVER", "city_name": "Denver", "country": "United States", "latitude": 39.7392, "longitude": -104.9903},
    {"city_id": "US-PHOENIX", "city_name": "Phoenix", "country": "United States", "latitude": 33.4484, "longitude": -112.0740},
    {"city_id": "US-AUSTIN", "city_name": "Austin", "country": "United States", "latitude": 30.2672, "longitude": -97.7431},
    {"city_id": "US-DALLAS", "city_name": "Dallas", "country": "United States", "latitude": 32.7767, "longitude": -96.7970},
    {"city_id": "US-HOUSTON", "city_name": "Houston", "country": "United States", "latitude": 29.7604, "longitude": -95.3698},
    {"city_id": "US-MIAMI", "city_name": "Miami", "country": "United States", "latitude": 25.7617, "longitude": -80.1918},
    {"city_id": "US-NEW_ORLEANS", "city_name": "New Orleans", "country": "United States", "latitude": 29.9511, "longitude": -90.0715},
]


def _company_metadata(ticker: str) -> tuple[str, str]:
    """Return company name and sector for disaster-sensitive industries."""
    lookup = {
        "ALL": ("Allstate Corporation", "Insurance"),
        "CB": ("Chubb Limited", "Insurance"),
        "TRV": ("Travelers Companies Inc.", "Insurance"),
        "AIG": ("American International Group", "Insurance"),
        "PGR": ("Progressive Corporation", "Insurance"),
        "BRK-B": ("Berkshire Hathaway Inc. Class B", "Insurance"),
        "HD": ("Home Depot Inc.", "Construction"),
        "LOW": ("Lowe's Companies Inc.", "Construction"),
        "CVX": ("Chevron Corporation", "Energy"),
        "XOM": ("Exxon Mobil Corporation", "Energy"),
    }
    return lookup.get(ticker.upper(), (ticker.upper(), "Unknown"))


def _load_city_reference() -> pd.DataFrame:
    """Load major city exposure points used for proximity-based disaster metrics."""
    if CITY_REFERENCE_PATH.exists():
        cities = pd.read_csv(CITY_REFERENCE_PATH)
    else:
        LOGGER.warning("City reference file not found at %s; using built-in fallback cities", CITY_REFERENCE_PATH)
        cities = pd.DataFrame(DEFAULT_CITY_ROWS)

    missing_cols = set(CITY_COLUMNS) - set(cities.columns)
    if missing_cols:
        raise ValueError(f"City reference is missing required columns: {sorted(missing_cols)}")

    cities = cities[CITY_COLUMNS].copy()
    cities["city_id"] = cities["city_id"].astype(str).str.strip()
    cities["city_name"] = cities["city_name"].astype(str).str.strip()
    cities["country"] = cities["country"].astype(str).str.strip()
    cities["latitude"] = pd.to_numeric(cities["latitude"], errors="coerce")
    cities["longitude"] = pd.to_numeric(cities["longitude"], errors="coerce")
    cities = cities.dropna(subset=["city_id", "city_name", "latitude", "longitude"])
    if cities.empty:
        raise ValueError("City reference has no valid rows after cleaning")
    return cities


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance between two latitude/longitude points."""
    radius_km = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    return 2 * radius_km * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _attach_nearest_city(flattened_df: pd.DataFrame, cities_df: pd.DataFrame) -> pd.DataFrame:
    """Add nearest city and distance fields to flattened EONET observations."""
    if flattened_df.empty:
        enriched = flattened_df.copy()
        enriched["nearest_city_id"] = pd.Series(dtype="object")
        enriched["nearest_city_name"] = pd.Series(dtype="object")
        enriched["nearest_city_country"] = pd.Series(dtype="object")
        enriched["nearest_city_latitude"] = pd.Series(dtype="float")
        enriched["nearest_city_longitude"] = pd.Series(dtype="float")
        enriched["nearest_disaster_distance_km"] = pd.Series(dtype="float")
        return enriched

    city_records = cities_df.to_dict("records")

    def nearest_city(row: pd.Series) -> pd.Series:
        if pd.isna(row["latitude"]) or pd.isna(row["longitude"]):
            return pd.Series(
                {
                    "nearest_city_id": None,
                    "nearest_city_name": None,
                    "nearest_city_country": None,
                    "nearest_city_latitude": None,
                    "nearest_city_longitude": None,
                    "nearest_disaster_distance_km": None,
                }
            )

        distances = [
            (
                city["city_id"],
                city["city_name"],
                city["country"],
                city["latitude"],
                city["longitude"],
                _haversine_km(float(row["latitude"]), float(row["longitude"]), float(city["latitude"]), float(city["longitude"])),
            )
            for city in city_records
        ]
        city_id, city_name, country, city_latitude, city_longitude, distance_km = min(distances, key=lambda item: item[5])
        return pd.Series(
            {
                "nearest_city_id": city_id,
                "nearest_city_name": city_name,
                "nearest_city_country": country,
                "nearest_city_latitude": city_latitude,
                "nearest_city_longitude": city_longitude,
                "nearest_disaster_distance_km": round(distance_km, 2),
            }
        )

    enriched = flattened_df.copy()
    city_metrics = enriched.apply(nearest_city, axis=1)
    return pd.concat([enriched, city_metrics], axis=1)


def _flatten_eonet_events(eonet_json_path: str) -> pd.DataFrame:
    """
    Parse and flatten the nested NASA EONET JSON structure.
    
    EONET JSON structure:
    {
        "events": [
            {
                "id": "EONET_12345",
                "title": "Wildfire - California",
                "categories": [{"id": 8, "title": "Wildfires"}],
                "geometries": [
                    {"date": "2024-03-15T00:00:00Z", "coordinates": [-120.5, 38.5]},
                    {"date": "2024-03-16T00:00:00Z", "coordinates": [-120.6, 38.6]}
                ]
            }
        ]
    }
    
    Returns a DataFrame where each row is a daily event instance with:
    - event_id, event_title, category_id, category_name, date, longitude, latitude
    """
    with open(eonet_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    events = data.get("events", [])
    if not events:
        return pd.DataFrame(columns=[
            "event_id", "event_title", "category_id", "category_name", 
            "date", "longitude", "latitude"
        ])
    
    # Step 1: Normalize the top-level events array
    df = pd.json_normalize(
        events,
        record_path=None,
        meta=["id", "title"],
        errors="ignore"
    )
    
    # Step 2: Extract category information (each event can have multiple categories, but we take the first)
    if "categories" not in df.columns:
        df["categories"] = [[] for _ in range(len(df))]

    df["category_id"] = df["categories"].apply(
        lambda cats: cats[0]["id"] if isinstance(cats, list) and cats else None
    )
    df["category_name"] = df["categories"].apply(
        lambda cats: cats[0]["title"] if isinstance(cats, list) and cats else None
    )

    # Step 3: Normalize geometry key (EONET v3 often uses "geometry", older payloads may use "geometries")
    geometry_col = "geometry" if "geometry" in df.columns else "geometries"
    if geometry_col not in df.columns:
        df["geometries"] = [[] for _ in range(len(df))]
    else:
        df["geometries"] = df[geometry_col].apply(lambda x: x if isinstance(x, list) else [])

    # Explode geometry points so each row = one daily observation of an event
    df = df.explode("geometries", ignore_index=True)
    
    # Step 4: Extract date and coordinates from each geometry
    df["date"] = df["geometries"].apply(
        lambda geo: geo.get("date") if isinstance(geo, dict) else None
    )
    df["coordinates"] = df["geometries"].apply(
        lambda geo: geo.get("coordinates") if isinstance(geo, dict) else None
    )
    
    # Step 5: Parse coordinates [longitude, latitude]
    df["longitude"] = df["coordinates"].apply(
        lambda coords: coords[0] if isinstance(coords, list) and len(coords) >= 2 else None
    )
    df["latitude"] = df["coordinates"].apply(
        lambda coords: coords[1] if isinstance(coords, list) and len(coords) >= 2 else None
    )
    
    # Step 6: Convert date strings to datetime and normalize to date
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True).dt.date
    
    # Step 7: Select final columns
    flattened = df[["id", "title", "category_id", "category_name", "date", "longitude", "latitude"]].copy()
    flattened.columns = ["event_id", "event_title", "category_id", "category_name", "date", "longitude", "latitude"]
    
    # Drop rows with missing dates
    flattened = flattened.dropna(subset=["date"])
    
    return flattened


def _aggregate_disaster_intensity(flattened_df: pd.DataFrame, relevant_categories: list[str]) -> pd.DataFrame:
    """
    Filter for relevant disaster categories and aggregate by date.
    
    Args:
        flattened_df: DataFrame with columns [event_id, category_name, date, ...]
        relevant_categories: List of category names to filter (e.g., ["Wildfires", "Severe Storms"])
    
    Returns:
        DataFrame with daily global and city-proximity disaster metrics.
    """
    if flattened_df.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "active_disaster_count",
                "nearby_disaster_count",
                "nearest_city_id",
                "nearest_city_name",
                "nearest_city_country",
                "nearest_city_latitude",
                "nearest_city_longitude",
                "nearest_disaster_distance_km",
            ]
        )
    
    # Filter for relevant categories (case-insensitive)
    filtered = flattened_df[
        flattened_df["category_name"].fillna("").str.lower().isin([cat.lower() for cat in relevant_categories])
    ].copy()
    
    if filtered.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "active_disaster_count",
                "nearby_disaster_count",
                "nearest_city_id",
                "nearest_city_name",
                "nearest_city_country",
                "nearest_city_latitude",
                "nearest_city_longitude",
                "nearest_disaster_distance_km",
            ]
        )

    filtered["is_near_city"] = filtered["nearest_disaster_distance_km"].le(CITY_IMPACT_RADIUS_KM)

    def nearest_city_field_for_day(group: pd.DataFrame, field_name: str):
        valid = group.dropna(subset=["nearest_disaster_distance_km"])
        if valid.empty:
            return None
        return valid.loc[valid["nearest_disaster_distance_km"].idxmin(), field_name]

    # Group by date and count unique events; one event can span multiple days.
    aggregated = filtered.groupby("date").apply(
        lambda group: pd.Series(
            {
                "active_disaster_count": group["event_id"].nunique(),
                "nearby_disaster_count": group.loc[group["is_near_city"], "event_id"].nunique(),
                "nearest_city_id": nearest_city_field_for_day(group, "nearest_city_id"),
                "nearest_city_name": nearest_city_field_for_day(group, "nearest_city_name"),
                "nearest_city_country": nearest_city_field_for_day(group, "nearest_city_country"),
                "nearest_city_latitude": nearest_city_field_for_day(group, "nearest_city_latitude"),
                "nearest_city_longitude": nearest_city_field_for_day(group, "nearest_city_longitude"),
                "nearest_disaster_distance_km": group["nearest_disaster_distance_km"].min(),
            }
        ),
    ).reset_index()
    
    return aggregated


def build_daily_datasets(market_path: str, eonet_path: str, ticker: str, execution_date: str) -> dict:
    """
    Build daily fact dataset correlating disaster intensity with stock performance.
    
    Args:
        market_path: Path to the market data JSON file (from extract.py)
        eonet_path: Path to the NASA EONET JSON file (from extract.py)
        ticker: Stock ticker symbol (e.g., "ALL", "HD", "CVX")
        execution_date: Date string in format YYYY-MM-DD
    
    Returns:
        Dictionary with paths to the generated CSV files
    """
    LOGGER.info(
        "Transform start | ticker=%s execution_date=%s market_path=%s eonet_path=%s",
        ticker,
        execution_date,
        market_path,
        eonet_path,
    )
    
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load market data
    try:
        market_df = pd.read_json(market_path)
        LOGGER.info("Market data loaded | ticker=%s rows=%s", ticker, len(market_df))
    except Exception as e:
        LOGGER.exception("Failed to load market data | ticker=%s", ticker)
        raise
    
    market_df["date"] = pd.to_datetime(market_df["date"], errors="coerce", utc=True).dt.tz_convert(None)
    market_df["open"] = pd.to_numeric(market_df["open"], errors="coerce")
    market_df["close"] = pd.to_numeric(market_df["close"], errors="coerce")
    market_df["volume"] = pd.to_numeric(market_df["volume"], errors="coerce")
    market_df = market_df.dropna(subset=["date", "open", "close", "volume"]).copy()
    market_df["date_normalized"] = market_df["date"].dt.date
    
    LOGGER.info("Market data cleaned | ticker=%s rows=%s", ticker, len(market_df))
    
    exec_ts = pd.to_datetime(execution_date)
    market_daily = market_df[market_df["date"] <= exec_ts].sort_values("date").tail(1).copy()
    
    if market_daily.empty:
        earliest_row = market_df.sort_values("date").head(1).copy()
        if earliest_row.empty:
            raise RuntimeError("No market data available after cleaning")

        fallback_date = earliest_row["date"].iloc[0]
        LOGGER.warning(
            "No market data row available on or before execution date; using earliest available date | "
            "ticker=%s execution_date=%s fallback_date=%s",
            ticker,
            execution_date,
            fallback_date.date(),
        )
        market_daily = earliest_row
    
    # Flatten and aggregate EONET disaster data
    try:
        flattened_eonet = _flatten_eonet_events(eonet_path)
        cities_df = _load_city_reference()
        flattened_eonet = _attach_nearest_city(flattened_eonet, cities_df)
        LOGGER.info("EONET data flattened and enriched | rows=%s cities=%s", len(flattened_eonet), len(cities_df))
    except Exception as e:
        LOGGER.exception("Failed to flatten/enrich EONET data")
        raise
    
    # Filter for relevant disaster categories
    relevant_categories = ["Wildfires", "Severe Storms"]
    disaster_daily = _aggregate_disaster_intensity(flattened_eonet, relevant_categories)
    LOGGER.info("Disaster aggregation complete | unique_dates=%s", len(disaster_daily))
    
    # If no disaster data, create empty aggregation
    if disaster_daily.empty:
        disaster_daily = pd.DataFrame({
            "date": [exec_ts.date()],
            "active_disaster_count": [0],
            "nearby_disaster_count": [0],
            "nearest_city_id": [None],
            "nearest_city_name": [None],
            "nearest_city_country": [None],
            "nearest_city_latitude": [None],
            "nearest_city_longitude": [None],
            "nearest_disaster_distance_km": [None],
        })
    
    # Ensure date is in date format for merging
    disaster_daily["date"] = pd.to_datetime(disaster_daily["date"]).dt.date
    
    # Merge market data with disaster data on date
    merged = pd.merge(
        market_daily,
        disaster_daily,
        left_on="date_normalized",
        right_on="date",
        how="left"
    )
    
    # Fill missing disaster counts with 0
    merged["active_disaster_count"] = merged["active_disaster_count"].fillna(0).astype(int)
    merged["nearby_disaster_count"] = merged["nearby_disaster_count"].fillna(0).astype(int)
    
    # Extract company metadata
    company_name, sector = _company_metadata(ticker)
    
    # Build final fact table
    output_df = pd.DataFrame({
        "date_key": merged["date_normalized"].astype(str),
        "ticker": ticker.upper(),
        "stock_close_price": merged["close"].astype(float),
        "stock_volume": merged["volume"].astype("int64"),
        "active_disaster_count": merged["active_disaster_count"],
        "nearby_disaster_count": merged["nearby_disaster_count"],
        "nearest_city_id": merged["nearest_city_id"],
        "nearest_city_name": merged["nearest_city_name"],
        "nearest_city_country": merged["nearest_city_country"],
        "nearest_city_latitude": merged["nearest_city_latitude"],
        "nearest_city_longitude": merged["nearest_city_longitude"],
        "nearest_disaster_distance_km": merged["nearest_disaster_distance_km"],
        "year": pd.to_datetime(merged["date_normalized"]).dt.year.astype(int),
        "month": pd.to_datetime(merged["date_normalized"]).dt.month.astype(int),
        "day": pd.to_datetime(merged["date_normalized"]).dt.day.astype(int),
        "is_weekend": pd.to_datetime(merged["date_normalized"]).dt.dayofweek >= 5,
        "company_name": company_name,
        "sector": sector,
    })
    
    # Save outputs
    out_path = PROCESSED_DIR / f"fact_daily_impact_{ticker}_{execution_date}.csv"
    output_df.to_csv(out_path, index=False)
    LOGGER.info("Fact output saved | ticker=%s path=%s rows=%s", ticker, out_path, len(output_df))
    
    # Optional: Save the flattened EONET data for debugging/validation
    eonet_detail_path = PROCESSED_DIR / f"eonet_flattened_{execution_date}.csv"
    if not flattened_eonet.empty:
        flattened_eonet.to_csv(eonet_detail_path, index=False)
        LOGGER.info("EONET detail output saved | path=%s rows=%s", eonet_detail_path, len(flattened_eonet))
    
    return {
        "fact_path": str(out_path),
        "eonet_detail_path": str(eonet_detail_path),
    }


def build_daily_dataset(market_path: str, eonet_path: str, ticker: str, execution_date: str) -> str:
    """Backward-compatible wrapper returning only the fact dataset path."""
    outputs = build_daily_datasets(
        market_path=market_path,
        eonet_path=eonet_path,
        ticker=ticker,
        execution_date=execution_date,
    )
    return outputs["fact_path"]


def build_daily_datasets_cached(
    market_path: str,
    eonet_path: str,
    ticker: str,
    execution_date: str,
    force_reprocess: bool = False,
) -> dict:
    """Cache-aware wrapper around build_daily_datasets.

    On the first run for a given date×ticker the full transform executes and
    writes ``fact_daily_impact_{ticker}_{execution_date}.csv`` to PROCESSED_DIR.
    On subsequent runs the file is detected and returned immediately (cache hit),
    reducing a 10-minute historical backfill to a few seconds.

    Args:
        market_path: Path to the raw market JSON file.
        eonet_path: Path to the raw EONET JSON file.
        ticker: Stock ticker symbol.
        execution_date: Date string in YYYY-MM-DD format.
        force_reprocess: When True, bypass the cache and re-run the full
            transform even if the output file already exists.  Can also be
            triggered by setting the ``FORCE_REPROCESS`` environment variable
            to ``"true"``.

    Returns:
        Dictionary with ``fact_path`` and ``eonet_detail_path`` keys.
    """
    env_force = os.getenv("FORCE_REPROCESS", "false").lower() == "true"
    should_force = force_reprocess or env_force

    out_path = PROCESSED_DIR / f"fact_daily_impact_{ticker}_{execution_date}.csv"
    eonet_detail_path = PROCESSED_DIR / f"eonet_flattened_{execution_date}.csv"

    if out_path.exists() and not should_force:
        LOGGER.info(
            "Cache hit — skipping transform | ticker=%s date=%s path=%s",
            ticker,
            execution_date,
            out_path,
        )
        return {
            "fact_path": str(out_path),
            "eonet_detail_path": str(eonet_detail_path),
        }

    if should_force:
        LOGGER.info(
            "Cache bypass requested | ticker=%s date=%s force_reprocess=%s",
            ticker,
            execution_date,
            should_force,
        )

    return build_daily_datasets(
        market_path=market_path,
        eonet_path=eonet_path,
        ticker=ticker,
        execution_date=execution_date,
    )
