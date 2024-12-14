WITH host_neighbourhood_lga AS (
    -- Map host_neighbourhood to the corresponding LGA
    SELECT
        h.host_neighbourhood,
        n.lga_name AS host_neighbourhood_lga,  -- Reference to n.lga_name
        l.month_year,  -- Aggregating by month and year
        COUNT(DISTINCT h.host_id) AS distinct_hosts,
        
        -- Calculate the total estimated revenue for each LGA
        SUM((30 - l.availability_30) * l.price) AS estimated_revenue,
        
        -- Calculate the average estimated revenue per host
        CASE 
            WHEN COUNT(DISTINCT h.host_id) > 0 THEN SUM((30 - l.availability_30) * l.price) / COUNT(DISTINCT h.host_id)
            ELSE 0
        END AS estimated_revenue_per_host
    FROM "postgres"."dbt_rsh_gold"."fact_listings" l
    LEFT JOIN "postgres"."dbt_rsh_gold"."dim_host" h ON l.host_id = h.host_id
    LEFT JOIN "postgres"."dbt_rsh_gold"."dim_neighbourhood" n ON h.host_neighbourhood = n.suburb_name
    GROUP BY h.host_neighbourhood, n.lga_name, l.month_year
)

SELECT
    host_neighbourhood_lga,
    month_year,
    distinct_hosts,
    estimated_revenue,
    estimated_revenue_per_host
FROM host_neighbourhood_lga
ORDER BY host_neighbourhood_lga, month_year