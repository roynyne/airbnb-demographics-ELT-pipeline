-- a) What are the demographic differences (e.g., age group distribution, household size) between the top 3 performing and lowest 3 performing LGAs based on estimated revenue per active listing over the last 12 months?


-- Step 1: Calculate the average estimated revenue per active listing by LGA for the last 12 months.
WITH lga_revenue AS (
    SELECT 
        host_neighbourhood_lga AS lga,
        SUM(estimated_revenue) / NULLIF(SUM(active_listings), 0) AS revenue_per_active_listing
    FROM gold.dm_host_neighbourhood
    WHERE month_year >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12 months'
    GROUP BY host_neighbourhood_lga
),

-- Step 2: Identify the top 3 and bottom 3 performing LGAs.
top_bottom_lgas AS (
    (SELECT lga FROM lga_revenue ORDER BY revenue_per_active_listing DESC LIMIT 3)
    UNION ALL
    (SELECT lga FROM lga_revenue ORDER BY revenue_per_active_listing ASC LIMIT 3)
)

-- Step 3: Fetch demographic data for the selected LGAs
SELECT 
    tb.lga,
    c1.total_population_male,
    c1.total_population_female,
    c2.median_age_persons,
    c2.average_household_size
FROM top_bottom_lgas tb
JOIN silver.s_census_g01 c1 ON tb.lga = c1.lga_code
JOIN silver.s_census_g02 c2 ON tb.lga = c2.lga_code;


---------------------------------------------------------


-- b) Is there a correlation between the median age of a neighbourhood (from Census data) and the revenue generated per active listing in that neighbourhood?


-- Step 1: Calculate revenue per active listing by neighborhood
WITH revenue_per_listing AS (
    SELECT 
        l.listing_neighbourhood AS neighbourhood,
        SUM(l.estimated_revenue) / NULLIF(SUM(l.active_listings), 0) AS revenue_per_active_listing
    FROM gold.dm_listing_neighbourhood l
    WHERE l.month_year >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12 months'
    GROUP BY l.listing_neighbourhood
),

-- Step 2: Retrieve the median age for each neighborhood from the Census table
neighbourhood_age AS (
    SELECT 
        n.suburb_name AS neighbourhood,
        c.median_age_persons
    FROM gold.dim_neighbourhood n
    JOIN silver.s_census_g02 c ON n.lga_code = c.lga_code
    WHERE c.median_age_persons IS NOT NULL
)

-- Step 3: Join revenue and age data
SELECT 
    r.neighbourhood,
    r.revenue_per_active_listing,
    a.median_age_persons
FROM revenue_per_listing r
JOIN neighbourhood_age a ON r.neighbourhood = a.neighbourhood;

-- To measure the correlation:

SELECT CORR(revenue_per_active_listing, median_age_persons)
FROM (
    SELECT 
        r.neighbourhood,
        r.revenue_per_active_listing,
        a.median_age_persons
    FROM revenue_per_listing r
    JOIN neighbourhood_age a ON r.neighbourhood = a.neighbourhood
) AS combined_data;


---------------------------------------------------------


-- c) What will be the best type of listing (property type, room type and accommodates for) for the top 5 “listing_neighbourhood” (in terms of estimated revenue per active listing) to have the highest number of stays?

-- Step 1: Identify the top 5 neighborhoods based on revenue per active listing
WITH top_neighbourhoods AS (
    SELECT 
        listing_neighbourhood,
        AVG(estimated_revenue_per_active_listing) AS avg_revenue_per_active_listing
    FROM gold.dm_listing_neighbourhood
    WHERE month_year >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12 months'
    GROUP BY listing_neighbourhood
    ORDER BY avg_revenue_per_active_listing DESC
    LIMIT 5
),

-- Step 2: Aggregate the number of stays for each property type, room type, and accommodates combination in the top neighborhoods
stays_by_listing_type AS (
    SELECT
        l.listing_neighbourhood,
        p.property_type,
        p.room_type,
        p.accommodates,
        SUM(l.total_stays) AS total_stays
    FROM gold.fact_listings l
    JOIN gold.dim_property_type p ON l.listing_id = p.listing_id
    WHERE l.listing_neighbourhood IN (SELECT listing_neighbourhood FROM top_neighbourhoods)
      AND l.month_year >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12 months'
    GROUP BY l.listing_neighbourhood, p.property_type, p.room_type, p.accommodates
)

-- Step 3: Select the best property type, room type, and accommodates combination based on total stays for each top neighborhood
SELECT 
    s.listing_neighbourhood,
    s.property_type,
    s.room_type,
    s.accommodates,
    s.total_stays
FROM (
    SELECT 
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        total_stays,
        RANK() OVER (PARTITION BY listing_neighbourhood ORDER BY total_stays DESC) AS rank_by_stays
    FROM stays_by_listing_type
) s
WHERE s.rank_by_stays = 1
ORDER BY s.listing_neighbourhood, s.total_stays DESC;


---------------------------------------------------------


-- d) For hosts with multiple listings, are their properties concentrated within the same LGA, or are they distributed across different LGAs?

-- Step 1: Identify hosts with multiple listings and count the distinct LGAs for each host
WITH multiple_listings AS (
    SELECT 
        h.host_id,
        COUNT(l.listing_id) AS total_listings,
        COUNT(DISTINCT n.lga_name) AS distinct_lgas
    FROM gold.fact_listings l
    JOIN gold.dim_host h ON l.host_id = h.host_id
    JOIN gold.dim_neighbourhood n ON l.listing_neighbourhood = n.suburb_name
    GROUP BY h.host_id
    HAVING COUNT(l.listing_id) > 1  -- Only consider hosts with multiple listings
)

-- Step 2: Classify if listings are concentrated in a single LGA or spread across multiple LGAs
SELECT 
    host_id,
    total_listings,
    distinct_lgas,
    CASE 
        WHEN distinct_lgas = 1 THEN 'Concentrated in one LGA'
        ELSE 'Spread across multiple LGAs'
    END AS lga_distribution
FROM multiple_listings
ORDER BY host_id;


---------------------------------------------------------


-- e) For hosts with a single Airbnb listing, does the estimated revenue over the last 12 months cover the annualised median mortgage repayment in the corresponding LGA? Which LGA has the highest percentage of hosts that can cover it?


-- Step 1: Calculate the estimated annual revenue for hosts with a single listing
WITH single_listing_hosts AS (
    SELECT 
        h.host_id,
        n.lga_name,
        l.listing_id,
        SUM(l.estimated_revenue) AS annual_revenue
    FROM dbt_rsh_gold.fact_listings l
    JOIN dbt_rsh_gold.dim_host h ON l.host_id = h.host_id
    JOIN dbt_rsh_gold.dim_neighbourhood n ON l.listing_neighbourhood = n.suburb_name
    -- Filter to only the last 12 months
    WHERE l.month_year >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12 months'
    GROUP BY h.host_id, n.lga_name, l.listing_id
    HAVING COUNT(l.listing_id) = 1  -- Only hosts with a single listing
),

-- Step 2: Retrieve median monthly mortgage data and annualize it
annual_mortgage AS (
    SELECT 
        lga_code,
        median_mortgage_repay_monthly * 12 AS annual_mortgage_repay
    FROM dbt_rsh_silver.s_census_g02  -- Assumes s_census_g02 has median mortgage repayment data
),

-- Step 3: Compare annual revenue with annualized mortgage for each LGA
coverage_analysis AS (
    SELECT
        s.host_id,
        s.lga_name,
        s.annual_revenue,
        a.annual_mortgage_repay,
        CASE WHEN s.annual_revenue >= a.annual_mortgage_repay THEN 1 ELSE 0 END AS can_cover_mortgage
    FROM single_listing_hosts s
    JOIN annual_mortgage a ON s.lga_name = a.lga_code
)

-- Step 4: Calculate the percentage of hosts in each LGA who can cover the mortgage repayment
SELECT 
    lga_name,
    COUNT(*) AS total_single_listing_hosts,
    SUM(can_cover_mortgage) AS hosts_covering_mortgage,
    ROUND(SUM(can_cover_mortgage)::DECIMAL / COUNT(*) * 100, 2) AS coverage_percentage
FROM coverage_analysis
GROUP BY lga_name
ORDER BY coverage_percentage DESC
LIMIT 1;  -- Get the LGA with the highest percentage of hosts covering the mortgage


