-- Database bootstrap for Airflow metadata, data warehouse, and Metabase metadata.

CREATE DATABASE airflow;
CREATE DATABASE disaster_dw;
CREATE DATABASE metabase;

\connect disaster_dw;

-- Dimension: Company (Stock Ticker) - Updated for disaster-sensitive industries
CREATE TABLE IF NOT EXISTS dim_company (
    ticker TEXT PRIMARY KEY,
    company_name TEXT NOT NULL,
    sector TEXT NOT NULL
);

-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key DATE PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Dimension: Event Category (NASA EONET Categories)
CREATE TABLE IF NOT EXISTS dim_event_category (
    category_id INTEGER PRIMARY KEY,
    category_name TEXT NOT NULL UNIQUE
);

-- Dimension: Major insured cities used for proximity-based disaster exposure
CREATE TABLE IF NOT EXISTS dim_city (
    city_id TEXT PRIMARY KEY,
    city_name TEXT NOT NULL,
    country TEXT NOT NULL,
    latitude NUMERIC(9, 6),
    longitude NUMERIC(9, 6)
);

-- Fact Table 1: Stock Daily Performance
CREATE TABLE IF NOT EXISTS fact_stock_daily (
    date_key DATE NOT NULL,
    ticker TEXT NOT NULL,
    stock_close_price NUMERIC(14, 4),
    stock_volume BIGINT,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    CONSTRAINT fact_stock_daily_pk PRIMARY KEY (date_key, ticker),
    CONSTRAINT fact_stock_daily_date_fk FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fact_stock_daily_company_fk FOREIGN KEY (ticker) REFERENCES dim_company(ticker)
);

-- Fact Table 2: City Disaster Daily
CREATE TABLE IF NOT EXISTS fact_city_disaster_daily (
    date_key DATE NOT NULL,
    city_id TEXT NOT NULL,
    active_disaster_count INTEGER DEFAULT 0,
    nearby_disaster_count INTEGER DEFAULT 0,
    nearest_disaster_distance_km NUMERIC(10, 2),
    is_nearby_disaster BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    CONSTRAINT fact_city_disaster_daily_pk PRIMARY KEY (date_key, city_id),
    CONSTRAINT fact_city_disaster_daily_date_fk FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fact_city_disaster_daily_city_fk FOREIGN KEY (city_id) REFERENCES dim_city(city_id)
);

-- Seed common EONET event categories for reference
INSERT INTO dim_event_category (category_id, category_name) VALUES
    (6, 'Drought'),
    (7, 'Dust and Haze'),
    (8, 'Wildfires'),
    (9, 'Floods'),
    (10, 'Severe Storms'),
    (12, 'Snow'),
    (13, 'Temperature Extremes'),
    (14, 'Volcanoes'),
    (15, 'Water Color'),
    (16, 'Landslides'),
    (17, 'Sea and Lake Ice'),
    (18, 'Earthquakes'),
    (19, 'Manmade')
ON CONFLICT (category_id) DO NOTHING;

-- Seed major cities with high property/casualty insurance exposure.
INSERT INTO dim_city (city_id, city_name, country, latitude, longitude) VALUES
    ('US-LOS_ANGELES', 'Los Angeles', 'United States', 34.0522, -118.2437),
    ('US-SAN_FRANCISCO', 'San Francisco', 'United States', 37.7749, -122.4194),
    ('US-SACRAMENTO', 'Sacramento', 'United States', 38.5816, -121.4944),
    ('US-DENVER', 'Denver', 'United States', 39.7392, -104.9903),
    ('US-PHOENIX', 'Phoenix', 'United States', 33.4484, -112.0740),
    ('US-AUSTIN', 'Austin', 'United States', 30.2672, -97.7431),
    ('US-DALLAS', 'Dallas', 'United States', 32.7767, -96.7970),
    ('US-HOUSTON', 'Houston', 'United States', 29.7604, -95.3698),
    ('US-MIAMI', 'Miami', 'United States', 25.7617, -80.1918),
    ('US-NEW_ORLEANS', 'New Orleans', 'United States', 29.9511, -90.0715)
ON CONFLICT (city_id) DO UPDATE
SET city_name = EXCLUDED.city_name,
    country = EXCLUDED.country,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude;

CREATE OR REPLACE VIEW vw_daily_disaster_stock_impact AS
SELECT
    s.date_key,
    d.year,
    d.month,
    d.day,
    c.ticker,
    c.company_name,
    c.sector,
    s.stock_close_price,
    s.stock_volume,
    city.city_id,
    city.city_name,
    city.country AS city_country,
    fcd.active_disaster_count,
    fcd.nearby_disaster_count,
    fcd.nearest_disaster_distance_km
FROM fact_stock_daily s
JOIN dim_date d ON d.date_key = s.date_key
JOIN dim_company c ON c.ticker = s.ticker
JOIN fact_city_disaster_daily fcd ON fcd.date_key = s.date_key
JOIN dim_city city ON city.city_id = fcd.city_id;

CREATE OR REPLACE VIEW vw_stock_disaster_price_movement AS
SELECT
    impact.*,
    LAG(stock_close_price) OVER (PARTITION BY ticker, city_id ORDER BY date_key) AS previous_close_price,
    ROUND(
        (
            (stock_close_price - LAG(stock_close_price) OVER (PARTITION BY ticker, city_id ORDER BY date_key))
            / NULLIF(LAG(stock_close_price) OVER (PARTITION BY ticker, city_id ORDER BY date_key), 0)
            * 100
        )::numeric,
        2
    ) AS pct_close_change
FROM vw_daily_disaster_stock_impact impact;

CREATE OR REPLACE VIEW vw_insurance_city_risk_days AS
SELECT
    date_key,
    ticker,
    company_name,
    stock_close_price,
    active_disaster_count,
    nearby_disaster_count,
    city_name,
    nearest_disaster_distance_km
FROM vw_daily_disaster_stock_impact
WHERE sector = 'Insurance'
  AND nearby_disaster_count > 0;

CREATE OR REPLACE VIEW vw_la_wildfire_insurance_risk_days AS
SELECT
    date_key,
    ticker,
    company_name,
    stock_close_price,
    active_disaster_count,
    nearby_disaster_count,
    city_name,
    nearest_disaster_distance_km
FROM vw_daily_disaster_stock_impact
WHERE sector = 'Insurance'
  AND city_id = 'US-LOS_ANGELES'
  AND nearby_disaster_count > 0;

CREATE OR REPLACE VIEW vw_la_disaster_insurance_risk_days AS
SELECT
    date_key,
    ticker,
    company_name,
    stock_close_price,
    active_disaster_count,
    nearby_disaster_count,
    city_name,
    nearest_disaster_distance_km
FROM vw_daily_disaster_stock_impact
WHERE sector = 'Insurance'
  AND city_id = 'US-LOS_ANGELES'
  AND nearby_disaster_count > 0;
