WITH property_metrics AS (
    SELECT
        p.property_type,
        p.room_type,
        p.accommodates,
        l.month_year,  -- Use month_year instead of scraped_date
        COUNT(*) AS total_listings,
        COUNT(CASE WHEN l.has_availability = 't' THEN 1 END) AS active_listings,
        COUNT(CASE WHEN l.has_availability = 'f' THEN 1 END) AS inactive_listings,
        MIN(l.price) AS min_price,
        MAX(l.price) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY l.price) AS median_price,  -- Using PERCENTILE_CONT for median
        AVG(l.price) AS avg_price,
        COUNT(DISTINCT l.host_id) AS distinct_hosts,
        AVG(l.review_scores_rating) AS avg_review_scores_rating,
        COUNT(CASE WHEN h.host_is_superhost = TRUE THEN 1 END) / COUNT(DISTINCT l.host_id) * 100 AS superhost_rate,
        
        -- Calculating number of stays for active listings
        SUM(CASE WHEN l.has_availability = 't' THEN 30 - l.availability_30 END) AS total_stays,
        
        -- Calculating average estimated revenue for active listings
        AVG(CASE WHEN l.has_availability = 't' THEN (30 - l.availability_30) * l.price END) AS avg_estimated_revenue_per_active_listing
    FROM "postgres"."dbt_rsh_gold"."fact_listings" l
    JOIN "postgres"."dbt_rsh_gold"."dim_property_type" p ON l.listing_id = p.listing_id
    JOIN "postgres"."dbt_rsh_gold"."dim_host" h ON l.host_id = h.host_id
    GROUP BY p.property_type, p.room_type, p.accommodates, l.month_year  -- Use l.month_year
),

-- Calculating percentage change for active listings
pct_change_active AS (
    SELECT
        property_type,
        room_type,
        accommodates,
        month_year,
        active_listings,
        LAG(active_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_year) AS prev_active_listings,
        (active_listings - LAG(active_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_year)) * 100.0 / 
        LAG(active_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_year) AS pct_change_active_listings
    FROM property_metrics
),

-- Calculating percentage change for inactive listings
pct_change_inactive AS (
    SELECT
        property_type,
        room_type,
        accommodates,
        month_year,
        inactive_listings,
        LAG(inactive_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_year) AS prev_inactive_listings,
        (inactive_listings - LAG(inactive_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_year)) * 100.0 / 
        LAG(inactive_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_year) AS pct_change_inactive_listings
    FROM property_metrics
)

-- Final select with all required metrics
SELECT
    pm.property_type,
    pm.room_type,
    pm.accommodates,
    pm.month_year,
    pm.active_listings,
    pm.inactive_listings,
    pm.total_listings,
    pm.min_price,
    pm.max_price,
    pm.median_price,
    pm.avg_price,
    pm.distinct_hosts,
    pm.superhost_rate,
    pm.avg_review_scores_rating,
    pm.total_stays,
    pm.avg_estimated_revenue_per_active_listing,
    pca.pct_change_active_listings,
    pci.pct_change_inactive_listings
FROM property_metrics pm
LEFT JOIN pct_change_active pca
    ON pm.property_type = pca.property_type AND pm.room_type = pca.room_type AND pm.accommodates = pca.accommodates AND pm.month_year = pca.month_year
LEFT JOIN pct_change_inactive pci
    ON pm.property_type = pci.property_type AND pm.room_type = pci.room_type AND pm.accommodates = pci.accommodates AND pm.month_year = pci.month_year
ORDER BY pm.property_type, pm.room_type, pm.accommodates, pm.month_year