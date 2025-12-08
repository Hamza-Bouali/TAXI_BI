-- Data Warehouse Schema for NYC Taxi Trip Data
-- Star Schema with Dimension and Fact Tables

-- =====================================================
-- DIMENSION TABLES
-- =====================================================

-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    day SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    season VARCHAR(10) NOT NULL
);

-- Create index on full_date for faster lookups
CREATE INDEX IF NOT EXISTS idx_dim_date_full_date ON dim_date(full_date);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON dim_date(year, month);

-- Dimension: Location
CREATE TABLE IF NOT EXISTS dim_location (
    location_key INT PRIMARY KEY,
    location_id INT UNIQUE,  -- Original LocationID from source data
    zone_name VARCHAR(255),
    borough VARCHAR(100),
    is_airport BOOLEAN DEFAULT FALSE,
    is_downtown BOOLEAN DEFAULT FALSE,
    is_tourist_area BOOLEAN DEFAULT FALSE,
    latitude FLOAT,
    longitude FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_location_id ON dim_location(location_id);
CREATE INDEX IF NOT EXISTS idx_dim_location_borough ON dim_location(borough);

-- Dimension: Vendor
CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_key INT PRIMARY KEY,
    vendor_id INT UNIQUE,  -- Original VendorID from source data
    vendor_name VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_vendor_id ON dim_vendor(vendor_id);

-- Dimension: Payment Type
CREATE TABLE IF NOT EXISTS dim_payment_type (
    payment_type_key INT PRIMARY KEY,
    payment_type_id INT UNIQUE,  -- Original payment_type from source data
    payment_name VARCHAR(100),
    is_electronic BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_payment_type_id ON dim_payment_type(payment_type_id);

-- Dimension: Rate Code
CREATE TABLE IF NOT EXISTS dim_rate_code (
    rate_code_key INT PRIMARY KEY,
    rate_code_id INT UNIQUE,  -- Original RatecodeID from source data
    rate_code_desc VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_rate_code_id ON dim_rate_code(rate_code_id);

-- =====================================================
-- FACT TABLE
-- =====================================================

-- Fact: Trips
CREATE TABLE IF NOT EXISTS fact_trips (
    trip_id BIGSERIAL PRIMARY KEY,
    vendor_key INT,
    pickup_location_key INT,
    dropoff_location_key INT,
    payment_type_key INT,
    rate_code_key INT,
    pickup_date_key INT,
    passenger_count SMALLINT,
    trip_distance_miles FLOAT,
    trip_duration_minutes SMALLINT,
    fare_amount DECIMAL(10,2),
    extra_amount DECIMAL(10,2),
    mta_tax DECIMAL(10,2),
    tip_amount DECIMAL(10,2),
    tolls_amount DECIMAL(10,2),
    improvement_surcharge DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    congestion_surcharge DECIMAL(10,2),
    trip_type SMALLINT,
    -- Source tracking
    source_file VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign Key Constraints
    CONSTRAINT fk_vendor FOREIGN KEY (vendor_key) REFERENCES dim_vendor(vendor_key),
    CONSTRAINT fk_pickup_location FOREIGN KEY (pickup_location_key) REFERENCES dim_location(location_key),
    CONSTRAINT fk_dropoff_location FOREIGN KEY (dropoff_location_key) REFERENCES dim_location(location_key),
    CONSTRAINT fk_payment_type FOREIGN KEY (payment_type_key) REFERENCES dim_payment_type(payment_type_key),
    CONSTRAINT fk_rate_code FOREIGN KEY (rate_code_key) REFERENCES dim_rate_code(rate_code_key),
    CONSTRAINT fk_pickup_date FOREIGN KEY (pickup_date_key) REFERENCES dim_date(date_key)
);

-- Indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_fact_trips_pickup_date ON fact_trips(pickup_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_trips_vendor ON fact_trips(vendor_key);
CREATE INDEX IF NOT EXISTS idx_fact_trips_pickup_location ON fact_trips(pickup_location_key);
CREATE INDEX IF NOT EXISTS idx_fact_trips_dropoff_location ON fact_trips(dropoff_location_key);
CREATE INDEX IF NOT EXISTS idx_fact_trips_payment_type ON fact_trips(payment_type_key);
CREATE INDEX IF NOT EXISTS idx_fact_trips_loaded_at ON fact_trips(loaded_at);

-- =====================================================
-- COMMENTS FOR DOCUMENTATION
-- =====================================================

COMMENT ON TABLE dim_date IS 'Date dimension for time-based analysis';
COMMENT ON TABLE dim_location IS 'Location dimension for pickup and dropoff analysis';
COMMENT ON TABLE dim_vendor IS 'Vendor dimension for taxi service providers';
COMMENT ON TABLE dim_payment_type IS 'Payment type dimension for payment method analysis';
COMMENT ON TABLE dim_rate_code IS 'Rate code dimension for pricing analysis';
COMMENT ON TABLE fact_trips IS 'Fact table containing trip transaction data';
