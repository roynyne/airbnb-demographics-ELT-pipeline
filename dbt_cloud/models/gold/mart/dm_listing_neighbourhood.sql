WITH active_listings AS (
    SELECT
        l.listing_neighbourhood,
        l.month_year,  -- Use month_year if scraped_date was renamed
        COUNT(*) AS total_listings,
        COUNT(CASE WHEN l.has_availability = 't' THEN 1 END) AS active_listings,
        COUNT(CASE WHEN l.has_availability = 'f' THEN 1 END) AS inactive_listings,
        MIN(l.price) AS min_price,
        MAX(l.price) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY l.price) AS median_price,
        AVG(l.price) AS avg_price,
        COUNT(DISTINCT l.host_id) AS distinct_hosts,
        AVG(l.review_scores_rating) AS avg_review_scores_rating,
        COUNT(CASE WHEN h.host_is_superhost = TRUE THEN 1 END) / COUNT(DISTINCT l.host_id) * 100 AS superhost_rate,
        
        -- Calculating number of stays
        SUM(CASE WHEN l.has_availability = 't' THEN 30 - l.availability_30 END) AS total_stays,
        
        -- Calculating estimated revenue for active listings
        AVG((30 - l.availability_30) * l.price) AS avg_estimated_revenue_per_active_listing
    FROM "postgres"."dbt_rsh_gold"."fact_listings" l
    JOIN "postgres"."dbt_rsh_gold"."dim_host" h ON l.host_id = h.host_id
    GROUP BY listing_neighbourhood, l.month_year  -- Use l.month_year here
),

-- Calculating percentage changes for active and inactive listings
pct_change_active AS (
    SELECT
        listing_neighbourhood,
        month_year,
        active_listings,
        LAG(active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year) AS prev_active_listings,
        (active_listings - LAG(active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year)) * 100.0 / 
        LAG(active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year) AS pct_change_active_listings
    FROM active_listings
),

pct_change_inactive AS (
    SELECT
        listing_neighbourhood,
        month_year,
        inactive_listings,
        LAG(inactive_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year) AS prev_inactive_listings,
        (inactive_listings - LAG(inactive_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year)) * 100.0 / 
        LAG(inactive_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year) AS pct_change_inactive_listings
    FROM active_listings
)

-- Final select with all the required metrics
SELECT
    a.listing_neighbourhood,
    a.month_year,
    a.active_listings,
    a.inactive_listings,
    a.total_listings,
    a.min_price,
    a.max_price,
    a.median_price,
    a.avg_price,
    a.distinct_hosts,
    a.superhost_rate,
    a.avg_review_scores_rating,
    a.total_stays,
    a.avg_estimated_revenue_per_active_listing,
    pca.pct_change_active_listings,
    pci.pct_change_inactive_listings
FROM active_listings a
LEFT JOIN pct_change_active pca
    ON a.listing_neighbourhood = pca.listing_neighbourhood AND a.month_year = pca.month_year
LEFT JOIN pct_change_inactive pci
    ON a.listing_neighbourhood = pci.listing_neighbourhood AND a.month_year = pci.month_year
ORDER BY a.listing_neighbourhood, a.month_year