-- Example Analytical Queries for NYC Taxi Data Warehouse

-- =====================================================
-- TEMPORAL ANALYSIS
-- =====================================================

-- 1. Daily trip trends with revenue
SELECT 
    d.full_date,
    d.day_name,
    d.is_weekend,
    COUNT(f.trip_id) as total_trips,
    SUM(f.total_amount) as daily_revenue,
    AVG(f.total_amount) as avg_fare,
    AVG(f.trip_distance_miles) as avg_distance,
    AVG(f.tip_amount) as avg_tip
FROM fact_trips f
JOIN dim_date d ON f.pickup_date_key = d.date_key
WHERE d.year = 2024
GROUP BY d.full_date, d.day_name, d.is_weekend
ORDER BY d.full_date;

-- 2. Weekend vs Weekday comparison
SELECT 
    d.is_weekend,
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
    COUNT(f.trip_id) as total_trips,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_fare,
    AVG(f.trip_duration_minutes) as avg_duration,
    AVG(f.tip_amount) as avg_tip
FROM fact_trips f
JOIN dim_date d ON f.pickup_date_key = d.date_key
GROUP BY d.is_weekend
ORDER BY d.is_weekend;

-- 3. Monthly trends
SELECT 
    d.year,
    d.month,
    TO_CHAR(d.full_date, 'Month') as month_name,
    COUNT(f.trip_id) as total_trips,
    SUM(f.total_amount) as total_revenue,
    AVG(f.trip_distance_miles) as avg_distance,
    SUM(f.passenger_count) as total_passengers
FROM fact_trips f
JOIN dim_date d ON f.pickup_date_key = d.date_key
GROUP BY d.year, d.month, TO_CHAR(d.full_date, 'Month')
ORDER BY d.year, d.month;

-- 4. Seasonal analysis
SELECT 
    d.season,
    COUNT(f.trip_id) as total_trips,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_fare,
    AVG(f.trip_distance_miles) as avg_distance
FROM fact_trips f
JOIN dim_date d ON f.pickup_date_key = d.date_key
GROUP BY d.season
ORDER BY total_trips DESC;

-- =====================================================
-- LOCATION ANALYSIS
-- =====================================================

-- 5. Top pickup locations
SELECT 
    l.zone_name,
    l.borough,
    COUNT(f.trip_id) as total_pickups,
    SUM(f.total_amount) as total_revenue,
    AVG(f.trip_distance_miles) as avg_trip_distance,
    AVG(f.total_amount) as avg_fare
FROM fact_trips f
JOIN dim_location l ON f.pickup_location_key = l.location_key
WHERE l.location_id > 0
GROUP BY l.zone_name, l.borough
ORDER BY total_pickups DESC
LIMIT 20;

-- 6. Top dropoff locations
SELECT 
    l.zone_name,
    l.borough,
    COUNT(f.trip_id) as total_dropoffs,
    SUM(f.total_amount) as total_revenue
FROM fact_trips f
JOIN dim_location l ON f.dropoff_location_key = l.location_key
WHERE l.location_id > 0
GROUP BY l.zone_name, l.borough
ORDER BY total_dropoffs DESC
LIMIT 20;

-- 7. Popular routes (pickup to dropoff)
SELECT 
    pl.zone_name as pickup_zone,
    dl.zone_name as dropoff_zone,
    pl.borough as pickup_borough,
    dl.borough as dropoff_borough,
    COUNT(f.trip_id) as trip_count,
    AVG(f.trip_distance_miles) as avg_distance,
    AVG(f.total_amount) as avg_fare,
    AVG(f.trip_duration_minutes) as avg_duration
FROM fact_trips f
JOIN dim_location pl ON f.pickup_location_key = pl.location_key
JOIN dim_location dl ON f.dropoff_location_key = dl.location_key
WHERE pl.location_id > 0 AND dl.location_id > 0
GROUP BY pl.zone_name, dl.zone_name, pl.borough, dl.borough
HAVING COUNT(f.trip_id) >= 100
ORDER BY trip_count DESC
LIMIT 30;

-- 8. Airport trips analysis
SELECT 
    CASE 
        WHEN pl.is_airport THEN 'From Airport'
        WHEN dl.is_airport THEN 'To Airport'
        ELSE 'Non-Airport'
    END as trip_type,
    COUNT(f.trip_id) as total_trips,
    AVG(f.total_amount) as avg_fare,
    AVG(f.trip_distance_miles) as avg_distance,
    AVG(f.trip_duration_minutes) as avg_duration
FROM fact_trips f
JOIN dim_location pl ON f.pickup_location_key = pl.location_key
JOIN dim_location dl ON f.dropoff_location_key = dl.location_key
GROUP BY 
    CASE 
        WHEN pl.is_airport THEN 'From Airport'
        WHEN dl.is_airport THEN 'To Airport'
        ELSE 'Non-Airport'
    END
ORDER BY total_trips DESC;

-- =====================================================
-- PAYMENT ANALYSIS
-- =====================================================

-- 9. Payment type distribution
SELECT 
    pt.payment_name,
    pt.is_electronic,
    COUNT(f.trip_id) as total_trips,
    ROUND(COUNT(f.trip_id)::NUMERIC * 100.0 / SUM(COUNT(f.trip_id)) OVER (), 2) as percentage,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_fare,
    AVG(f.tip_amount) as avg_tip
FROM fact_trips f
JOIN dim_payment_type pt ON f.payment_type_key = pt.payment_type_key
GROUP BY pt.payment_name, pt.is_electronic
ORDER BY total_trips DESC;

-- 10. Tipping behavior by payment type
SELECT 
    pt.payment_name,
    COUNT(f.trip_id) as total_trips,
    AVG(f.fare_amount) as avg_fare,
    AVG(f.tip_amount) as avg_tip,
    CASE 
        WHEN AVG(f.fare_amount) > 0 
        THEN ROUND(AVG(f.tip_amount) / AVG(f.fare_amount) * 100, 2)
        ELSE 0
    END as avg_tip_percentage,
    SUM(CASE WHEN f.tip_amount > 0 THEN 1 ELSE 0 END) as trips_with_tip,
    ROUND(SUM(CASE WHEN f.tip_amount > 0 THEN 1 ELSE 0 END)::NUMERIC * 100.0 / COUNT(f.trip_id), 2) as tip_rate_percentage
FROM fact_trips f
JOIN dim_payment_type pt ON f.payment_type_key = pt.payment_type_key
GROUP BY pt.payment_name
ORDER BY avg_tip_percentage DESC;

-- =====================================================
-- VENDOR ANALYSIS
-- =====================================================

-- 11. Vendor performance comparison
SELECT 
    v.vendor_name,
    COUNT(f.trip_id) as total_trips,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_fare,
    AVG(f.trip_distance_miles) as avg_distance,
    AVG(f.trip_duration_minutes) as avg_duration,
    AVG(f.passenger_count) as avg_passengers
FROM fact_trips f
JOIN dim_vendor v ON f.vendor_key = v.vendor_key
WHERE v.vendor_id > 0
GROUP BY v.vendor_name
ORDER BY total_trips DESC;

-- =====================================================
-- RATE CODE ANALYSIS
-- =====================================================

-- 12. Rate code usage
SELECT 
    rc.rate_code_desc,
    COUNT(f.trip_id) as total_trips,
    ROUND(COUNT(f.trip_id)::NUMERIC * 100.0 / SUM(COUNT(f.trip_id)) OVER (), 2) as percentage,
    AVG(f.total_amount) as avg_fare,
    AVG(f.trip_distance_miles) as avg_distance
FROM fact_trips f
JOIN dim_rate_code rc ON f.rate_code_key = rc.rate_code_key
GROUP BY rc.rate_code_desc
ORDER BY total_trips DESC;

-- =====================================================
-- PASSENGER ANALYSIS
-- =====================================================

-- 13. Passenger count distribution
SELECT 
    f.passenger_count,
    COUNT(f.trip_id) as total_trips,
    ROUND(COUNT(f.trip_id)::NUMERIC * 100.0 / SUM(COUNT(f.trip_id)) OVER (), 2) as percentage,
    AVG(f.total_amount) as avg_fare,
    AVG(f.trip_distance_miles) as avg_distance
FROM fact_trips f
WHERE f.passenger_count > 0
GROUP BY f.passenger_count
ORDER BY f.passenger_count;

-- =====================================================
-- REVENUE ANALYSIS
-- =====================================================

-- 14. Revenue breakdown by component
SELECT 
    COUNT(f.trip_id) as total_trips,
    SUM(f.fare_amount) as total_base_fare,
    SUM(f.extra_amount) as total_extra,
    SUM(f.mta_tax) as total_mta_tax,
    SUM(f.tip_amount) as total_tips,
    SUM(f.tolls_amount) as total_tolls,
    SUM(f.improvement_surcharge) as total_improvement_surcharge,
    SUM(f.congestion_surcharge) as total_congestion_surcharge,
    SUM(f.total_amount) as total_revenue,
    ROUND(SUM(f.tip_amount) / NULLIF(SUM(f.total_amount), 0) * 100, 2) as tip_percentage_of_total
FROM fact_trips f;

-- 15. Top revenue generating days
SELECT 
    d.full_date,
    d.day_name,
    COUNT(f.trip_id) as total_trips,
    SUM(f.total_amount) as daily_revenue,
    AVG(f.total_amount) as avg_fare
FROM fact_trips f
JOIN dim_date d ON f.pickup_date_key = d.date_key
GROUP BY d.full_date, d.day_name
ORDER BY daily_revenue DESC
LIMIT 20;

-- =====================================================
-- EFFICIENCY METRICS
-- =====================================================

-- 16. Trip efficiency (revenue per mile and per minute)
SELECT 
    CASE 
        WHEN f.trip_distance_miles < 1 THEN '< 1 mile'
        WHEN f.trip_distance_miles < 5 THEN '1-5 miles'
        WHEN f.trip_distance_miles < 10 THEN '5-10 miles'
        ELSE '10+ miles'
    END as distance_category,
    COUNT(f.trip_id) as total_trips,
    AVG(f.total_amount / NULLIF(f.trip_distance_miles, 0)) as revenue_per_mile,
    AVG(f.total_amount / NULLIF(f.trip_duration_minutes, 0)) as revenue_per_minute,
    AVG(f.trip_distance_miles) as avg_distance,
    AVG(f.trip_duration_minutes) as avg_duration,
    AVG(f.total_amount) as avg_fare
FROM fact_trips f
WHERE f.trip_distance_miles > 0 AND f.trip_duration_minutes > 0
GROUP BY 
    CASE 
        WHEN f.trip_distance_miles < 1 THEN '< 1 mile'
        WHEN f.trip_distance_miles < 5 THEN '1-5 miles'
        WHEN f.trip_distance_miles < 10 THEN '5-10 miles'
        ELSE '10+ miles'
    END
ORDER BY 
    CASE 
        WHEN distance_category = '< 1 mile' THEN 1
        WHEN distance_category = '1-5 miles' THEN 2
        WHEN distance_category = '5-10 miles' THEN 3
        ELSE 4
    END;

-- =====================================================
-- COHORT ANALYSIS
-- =====================================================

-- 17. Compare metrics across different time periods
WITH monthly_metrics AS (
    SELECT 
        d.year,
        d.month,
        COUNT(f.trip_id) as trips,
        SUM(f.total_amount) as revenue,
        AVG(f.total_amount) as avg_fare,
        AVG(f.trip_distance_miles) as avg_distance
    FROM fact_trips f
    JOIN dim_date d ON f.pickup_date_key = d.date_key
    GROUP BY d.year, d.month
)
SELECT 
    year,
    month,
    trips,
    revenue,
    avg_fare,
    avg_distance,
    trips - LAG(trips) OVER (ORDER BY year, month) as trip_change,
    ROUND((trips - LAG(trips) OVER (ORDER BY year, month))::NUMERIC * 100.0 / 
          NULLIF(LAG(trips) OVER (ORDER BY year, month), 0), 2) as trip_change_pct
FROM monthly_metrics
ORDER BY year, month;

-- =====================================================
-- DATA QUALITY MONITORING
-- =====================================================

-- 18. Data completeness check
SELECT 
    source_file,
    COUNT(*) as total_records,
    COUNT(CASE WHEN vendor_key = -1 THEN 1 END) as missing_vendor,
    COUNT(CASE WHEN pickup_location_key = -1 THEN 1 END) as missing_pickup_location,
    COUNT(CASE WHEN dropoff_location_key = -1 THEN 1 END) as missing_dropoff_location,
    COUNT(CASE WHEN payment_type_key = -1 THEN 1 END) as missing_payment_type,
    MIN(loaded_at) as first_loaded,
    MAX(loaded_at) as last_loaded
FROM fact_trips
GROUP BY source_file
ORDER BY source_file;
