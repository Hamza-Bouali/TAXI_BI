-- Helper Functions and Procedures for Data Warehouse Operations

-- =====================================================
-- Function: Generate Date Dimension
-- =====================================================
CREATE OR REPLACE FUNCTION populate_dim_date(start_date DATE, end_date DATE)
RETURNS INTEGER AS $$
DECLARE
    current_date DATE := start_date;
    records_inserted INTEGER := 0;
BEGIN
    WHILE current_date <= end_date LOOP
        INSERT INTO dim_date (
            date_key, 
            full_date, 
            year, 
            quarter, 
            month, 
            day, 
            day_of_week, 
            day_name, 
            is_weekend, 
            season
        )
        VALUES (
            TO_CHAR(current_date, 'YYYYMMDD')::INTEGER,
            current_date,
            EXTRACT(YEAR FROM current_date)::SMALLINT,
            EXTRACT(QUARTER FROM current_date)::SMALLINT,
            EXTRACT(MONTH FROM current_date)::SMALLINT,
            EXTRACT(DAY FROM current_date)::SMALLINT,
            EXTRACT(DOW FROM current_date)::SMALLINT,
            TO_CHAR(current_date, 'Day'),
            CASE WHEN EXTRACT(DOW FROM current_date) IN (0, 6) THEN TRUE ELSE FALSE END,
            CASE 
                WHEN EXTRACT(MONTH FROM current_date) IN (12, 1, 2) THEN 'Winter'
                WHEN EXTRACT(MONTH FROM current_date) IN (3, 4, 5) THEN 'Spring'
                WHEN EXTRACT(MONTH FROM current_date) IN (6, 7, 8) THEN 'Summer'
                ELSE 'Fall'
            END
        )
        ON CONFLICT (full_date) DO NOTHING;
        
        records_inserted := records_inserted + 1;
        current_date := current_date + INTERVAL '1 day';
    END LOOP;
    
    RETURN records_inserted;
END;
$$ LANGUAGE plpgsql;

-- Generate dates from 2019 to 2030
SELECT populate_dim_date('2019-01-01'::DATE, '2030-12-31'::DATE);

-- =====================================================
-- View: Trip Summary by Date
-- =====================================================
CREATE OR REPLACE VIEW vw_trip_summary_by_date AS
SELECT 
    d.full_date,
    d.day_name,
    d.is_weekend,
    COUNT(f.trip_id) as total_trips,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_fare,
    AVG(f.trip_distance_miles) as avg_distance,
    AVG(f.trip_duration_minutes) as avg_duration,
    SUM(f.passenger_count) as total_passengers
FROM fact_trips f
JOIN dim_date d ON f.pickup_date_key = d.date_key
GROUP BY d.full_date, d.day_name, d.is_weekend
ORDER BY d.full_date;

-- =====================================================
-- View: Trip Summary by Location
-- =====================================================
CREATE OR REPLACE VIEW vw_trip_summary_by_location AS
SELECT 
    l.zone_name,
    l.borough,
    COUNT(f.trip_id) as total_trips,
    SUM(f.total_amount) as total_revenue,
    AVG(f.trip_distance_miles) as avg_distance
FROM fact_trips f
JOIN dim_location l ON f.pickup_location_key = l.location_key
WHERE l.location_id != -1
GROUP BY l.zone_name, l.borough
ORDER BY total_trips DESC;

-- =====================================================
-- View: Payment Type Analysis
-- =====================================================
CREATE OR REPLACE VIEW vw_payment_analysis AS
SELECT 
    pt.payment_name,
    pt.is_electronic,
    COUNT(f.trip_id) as total_trips,
    SUM(f.total_amount) as total_revenue,
    AVG(f.tip_amount) as avg_tip,
    ROUND(COUNT(f.trip_id)::NUMERIC * 100.0 / SUM(COUNT(f.trip_id)) OVER (), 2) as percentage
FROM fact_trips f
JOIN dim_payment_type pt ON f.payment_type_key = pt.payment_type_key
GROUP BY pt.payment_name, pt.is_electronic
ORDER BY total_trips DESC;

-- =====================================================
-- Function: Get or Create Location Key
-- =====================================================
CREATE OR REPLACE FUNCTION get_or_create_location_key(loc_id INTEGER)
RETURNS INTEGER AS $$
DECLARE
    loc_key INTEGER;
BEGIN
    -- Try to find existing location
    SELECT location_key INTO loc_key
    FROM dim_location
    WHERE location_id = loc_id;
    
    -- If not found, create new location
    IF NOT FOUND THEN
        INSERT INTO dim_location (location_key, location_id, zone_name, borough)
        VALUES (
            COALESCE((SELECT MAX(location_key) FROM dim_location WHERE location_key > 0), 0) + 1,
            loc_id,
            'Zone ' || loc_id,
            'Unknown'
        )
        RETURNING location_key INTO loc_key;
    END IF;
    
    RETURN loc_key;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- Data Quality Check Functions
-- =====================================================

-- Check for orphan records in fact table
CREATE OR REPLACE VIEW vw_data_quality_orphans AS
SELECT 
    'Missing Vendor' as issue_type,
    COUNT(*) as issue_count
FROM fact_trips f
LEFT JOIN dim_vendor v ON f.vendor_key = v.vendor_key
WHERE v.vendor_key IS NULL

UNION ALL

SELECT 
    'Missing Pickup Location' as issue_type,
    COUNT(*) as issue_count
FROM fact_trips f
LEFT JOIN dim_location l ON f.pickup_location_key = l.location_key
WHERE l.location_key IS NULL

UNION ALL

SELECT 
    'Missing Dropoff Location' as issue_type,
    COUNT(*) as issue_count
FROM fact_trips f
LEFT JOIN dim_location l ON f.dropoff_location_key = l.location_key
WHERE l.location_key IS NULL

UNION ALL

SELECT 
    'Missing Payment Type' as issue_type,
    COUNT(*) as issue_count
FROM fact_trips f
LEFT JOIN dim_payment_type pt ON f.payment_type_key = pt.payment_type_key
WHERE pt.payment_type_key IS NULL;
