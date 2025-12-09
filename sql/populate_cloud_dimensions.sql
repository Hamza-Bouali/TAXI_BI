-- =====================================================
-- Populate Cloud Data Warehouse Dimensions
-- Schema without _id columns (simplified surrogate keys)
-- =====================================================

-- =====================================================
-- Populate dim_vendor
-- =====================================================
INSERT INTO dim_vendor (vendor_key, vendor_name, is_active)
VALUES
    (1, 'Creative Mobile Technologies', TRUE),
    (2, 'VeriFone Inc.', TRUE),
    (-1, 'Unknown', FALSE)
ON CONFLICT (vendor_key) DO NOTHING;

-- =====================================================
-- Populate dim_payment_type
-- =====================================================
INSERT INTO dim_payment_type (payment_type_key, payment_name, is_electronic)
VALUES
    (1, 'Credit card', TRUE),
    (2, 'Cash', FALSE),
    (3, 'No charge', FALSE),
    (4, 'Dispute', FALSE),
    (5, 'Unknown', FALSE),
    (6, 'Voided trip', FALSE),
    (-1, 'Unknown', FALSE)
ON CONFLICT (payment_type_key) DO NOTHING;

-- =====================================================
-- Populate dim_rate_code
-- =====================================================
INSERT INTO dim_rate_code (rate_code_key, rate_code_desc)
VALUES
    (1, 'Standard rate'),
    (2, 'JFK'),
    (3, 'Newark'),
    (4, 'Nassau or Westchester'),
    (5, 'Negotiated fare'),
    (6, 'Group ride'),
    (-1, 'Unknown')
ON CONFLICT (rate_code_key) DO NOTHING;

-- =====================================================
-- Populate dim_location (NYC Taxi Zones)
-- =====================================================
INSERT INTO dim_location (location_key, zone_name, borough, is_airport, is_downtown, is_tourist_area, latitude, longitude)
VALUES
    (1, 'Newark Airport', 'EWR', TRUE, FALSE, FALSE, 40.6895, -74.1745),
    (2, 'Jamaica Bay', 'Queens', FALSE, FALSE, FALSE, 40.6095, -73.8240),
    (3, 'Allerton/Pelham Gardens', 'Bronx', FALSE, FALSE, FALSE, 40.8656, -73.8475),
    (4, 'Alphabet City', 'Manhattan', FALSE, FALSE, TRUE, 40.7250, -73.9780),
    (5, 'Arden Heights', 'Staten Island', FALSE, FALSE, FALSE, 40.5565, -74.1844),
    (6, 'Arrochar/Fort Wadsworth', 'Staten Island', FALSE, FALSE, FALSE, 40.6041, -74.0627),
    (7, 'Astoria', 'Queens', FALSE, FALSE, FALSE, 40.7720, -73.9300),
    (8, 'Astoria Park', 'Queens', FALSE, FALSE, FALSE, 40.7788, -73.9252),
    (9, 'Auburndale', 'Queens', FALSE, FALSE, FALSE, 40.7561, -73.7956),
    (10, 'Baisley Park', 'Queens', FALSE, FALSE, FALSE, 40.6850, -73.7775),
    (132, 'JFK Airport', 'Queens', TRUE, FALSE, FALSE, 40.6413, -73.7781),
    (138, 'LaGuardia Airport', 'Queens', TRUE, FALSE, FALSE, 40.7769, -73.8740),
    (161, 'Midtown Center', 'Manhattan', FALSE, TRUE, TRUE, 40.7549, -73.9840),
    (162, 'Midtown East', 'Manhattan', FALSE, TRUE, TRUE, 40.7549, -73.9712),
    (163, 'Midtown North', 'Manhattan', FALSE, TRUE, TRUE, 40.7614, -73.9776),
    (164, 'Midtown South', 'Manhattan', FALSE, TRUE, TRUE, 40.7505, -73.9860),
    (186, 'Penn Station/Madison Sq West', 'Manhattan', FALSE, TRUE, TRUE, 40.7505, -73.9934),
    (230, 'Times Sq/Theatre District', 'Manhattan', FALSE, TRUE, TRUE, 40.7590, -73.9845),
    (237, 'Upper East Side North', 'Manhattan', FALSE, FALSE, TRUE, 40.7736, -73.9566),
    (238, 'Upper East Side South', 'Manhattan', FALSE, FALSE, TRUE, 40.7682, -73.9626),
    (239, 'Upper West Side North', 'Manhattan', FALSE, FALSE, TRUE, 40.7870, -73.9754),
    (240, 'Upper West Side South', 'Manhattan', FALSE, FALSE, TRUE, 40.7870, -73.9754),
    (249, 'West Village', 'Manhattan', FALSE, FALSE, TRUE, 40.7358, -74.0036),
    (261, 'World Trade Center', 'Manhattan', FALSE, TRUE, TRUE, 40.7127, -74.0134),
    (264, 'Unknown', 'Unknown', FALSE, FALSE, FALSE, NULL, NULL),
    (265, 'NA', 'Unknown', FALSE, FALSE, FALSE, NULL, NULL),
    (-1, 'Unknown', 'Unknown', FALSE, FALSE, FALSE, NULL, NULL)
ON CONFLICT (location_key) DO NOTHING;

-- Add more locations as needed (265 total zones available)
