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


def _relevant_categories() -> list[str] | None:
    """Return configured EONET categories; ALL disables category filtering."""
    raw = os.getenv(
        "RELEVANT_EVENT_CATEGORIES",
        "Wildfires,Severe Storms,Floods,Landslides,Earthquakes,Temperature Extremes,Drought",
    )
    categories = [category.strip() for category in raw.split(",") if category.strip()]
    if any(category.upper() == "ALL" for category in categories):
        return None
    return categories


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


def build_city_disaster_daily(flattened_eonet: pd.DataFrame, cities_df: pd.DataFrame, execution_date: str) -> pd.DataFrame:
    exec_ts = pd.to_datetime(execution_date).date()
    
    # Filter EONET data for the execution date
    if not flattened_eonet.empty:
        day_events = flattened_eonet[flattened_eonet["date"] == exec_ts].copy()
    else:
        day_events = pd.DataFrame()
    
    # Filter for configured disaster categories. This is intentionally city-independent:
    # a Los Angeles risk day is counted from disasters near Los Angeles even if another
    # same-day event is closer to a different city.
    relevant_categories = _relevant_categories()
    if not day_events.empty:
        if relevant_categories is not None:
            day_events = day_events[
                day_events["category_name"].fillna("").str.lower().isin([cat.lower() for cat in relevant_categories])
            ]
    
    active_disaster_count = day_events["event_id"].nunique() if not day_events.empty else 0
    
    city_records = cities_df.to_dict("records")
    results = []
    
    for city in city_records:
        city_id = city["city_id"]
        
        if day_events.empty:
            min_dist = None
            nearby_count = 0
        else:
            # calculate distance to ALL events
            distances = []
            for _, row in day_events.iterrows():
                if pd.notna(row["latitude"]) and pd.notna(row["longitude"]):
                    dist = _haversine_km(float(row["latitude"]), float(row["longitude"]), float(city["latitude"]), float(city["longitude"]))
                    distances.append((row["event_id"], dist))
            
            if not distances:
                min_dist = None
                nearby_count = 0
            else:
                min_dist = min(d[1] for d in distances)
                # Count unique events within impact radius
                nearby_events = {d[0] for d in distances if d[1] <= CITY_IMPACT_RADIUS_KM}
                nearby_count = len(nearby_events)
                
        results.append({
            "date_key": str(exec_ts),
            "city_id": city_id,
            "active_disaster_count": active_disaster_count,
            "nearby_disaster_count": nearby_count,
            "nearest_disaster_distance_km": round(min_dist, 2) if min_dist is not None else None,
            "is_nearby_disaster": nearby_count > 0,
            "year": exec_ts.year,
            "month": exec_ts.month,
            "day": exec_ts.day,
            "is_weekend": exec_ts.weekday() >= 5
        })
        
    return pd.DataFrame(results)

def build_daily_datasets(market_path: str, eonet_path: str, ticker: str, execution_date: str) -> dict:
    """
    Build daily fact datasets for stock performance and city disasters.
    """
    LOGGER.info(
        "Transform start | ticker=%s execution_date=%s market_path=%s eonet_path=%s",
        ticker,
        execution_date,
        market_path,
        eonet_path,
    )
    
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Build Stock CSV
    try:
        market_df = pd.read_json(market_path)
    except Exception as e:
        LOGGER.exception("Failed to load market data | ticker=%s", ticker)
        raise
    
    market_df["date"] = pd.to_datetime(market_df["date"], errors="coerce", utc=True).dt.tz_convert(None)
    market_df["open"] = pd.to_numeric(market_df["open"], errors="coerce")
    market_df["close"] = pd.to_numeric(market_df["close"], errors="coerce")
    market_df["volume"] = pd.to_numeric(market_df["volume"], errors="coerce")
    market_df = market_df.dropna(subset=["date", "open", "close", "volume"]).copy()
    market_df["date_normalized"] = market_df["date"].dt.date
    
    exec_ts = pd.to_datetime(execution_date).date()
    market_daily = market_df[market_df["date_normalized"] == exec_ts].copy()
    
    stock_out_path = PROCESSED_DIR / f"fact_stock_daily_{ticker}_{execution_date}.csv"
    if not market_daily.empty:
        row = market_daily.iloc[0]
        stock_df = pd.DataFrame([{
            "date_key": str(row["date_normalized"]),
            "ticker": ticker.upper(),
            "stock_close_price": float(row["close"]),
            "stock_volume": int(row["volume"])
        }])
        stock_df.to_csv(stock_out_path, index=False)
        LOGGER.info("Stock fact output saved | ticker=%s path=%s rows=%s", ticker, stock_out_path, len(stock_df))
    else:
        pd.DataFrame(columns=["date_key", "ticker", "stock_close_price", "stock_volume"]).to_csv(stock_out_path, index=False)
        LOGGER.info("Stock fact output saved (empty) | ticker=%s path=%s", ticker, stock_out_path)
        
    # 2. Build City Disaster CSV
    try:
        flattened_eonet = _flatten_eonet_events(eonet_path)
        cities_df = _load_city_reference()
    except Exception as e:
        LOGGER.exception("Failed to flatten EONET data")
        raise
        
    city_disaster_df = build_city_disaster_daily(flattened_eonet, cities_df, execution_date)
    city_out_path = PROCESSED_DIR / f"fact_city_disaster_daily_{execution_date}.csv"
    
    # Overwrites on multiple tickers for same date, but contents are identical.
    city_disaster_df.to_csv(city_out_path, index=False)
    LOGGER.info("City disaster fact output saved | path=%s rows=%s", city_out_path, len(city_disaster_df))
    
    # Optional: Save the flattened EONET data for debugging/validation
    eonet_detail_path = PROCESSED_DIR / f"eonet_flattened_{execution_date}.csv"
    if not flattened_eonet.empty:
        flattened_eonet.to_csv(eonet_detail_path, index=False)
    
    return {
        "fact_stock_path": str(stock_out_path),
        "fact_city_disaster_path": str(city_out_path),
        "eonet_detail_path": str(eonet_detail_path),
    }

def build_daily_datasets_cached(
    market_path: str,
    eonet_path: str,
    ticker: str,
    execution_date: str,
    force_reprocess: bool = False,
) -> dict:
    env_force = os.getenv("FORCE_REPROCESS", "false").lower() == "true"
    should_force = force_reprocess or env_force

    stock_out_path = PROCESSED_DIR / f"fact_stock_daily_{ticker}_{execution_date}.csv"
    city_out_path = PROCESSED_DIR / f"fact_city_disaster_daily_{execution_date}.csv"
    eonet_detail_path = PROCESSED_DIR / f"eonet_flattened_{execution_date}.csv"

    if stock_out_path.exists() and city_out_path.exists() and not should_force:
        LOGGER.info(
            "Cache hit — skipping transform | ticker=%s date=%s",
            ticker,
            execution_date,
        )
        return {
            "fact_stock_path": str(stock_out_path),
            "fact_city_disaster_path": str(city_out_path),
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
