WITH source AS (
    SELECT * 
    FROM {{ ref('b_airbnb_listings') }}
),

-- Cleaning and standardizing the data
cleaned AS (
    SELECT
        CAST(listing_id AS INTEGER) AS listing_id,
        scrape_id,
        TO_DATE(scraped_date, 'YYYY-MM-DD') AS scraped_date,
        CAST(host_id AS INTEGER) AS host_id,
        COALESCE(host_name, 'Unknown') AS host_name,  -- Handling missing host names
        
        -- Handling invalid date formats for host_since
        CASE 
            WHEN host_since ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(host_since, 'DD/MM/YYYY') 
            ELSE NULL  -- Set invalid dates to NULL
        END AS host_since,
        
        CASE 
            WHEN host_is_superhost = 't' THEN TRUE 
            WHEN host_is_superhost = 'f' THEN FALSE 
            ELSE NULL  -- Handle unexpected values
        END AS host_is_superhost,
        
        UPPER(COALESCE(host_neighbourhood, 'Unknown')) AS host_neighbourhood,  -- Handling missing neighbourhoods
        listing_neighbourhood,
        property_type,
        room_type,
        CAST(accommodates AS INTEGER) AS accommodates,
        
        -- Cast price to NUMERIC before performing comparison
        CASE 
            WHEN CAST(price AS NUMERIC) >= 0 AND CAST(price AS NUMERIC) <= 10000 THEN CAST(price AS NUMERIC)
            ELSE NULL  -- Nullify extreme or negative values
        END AS price,
        
        CASE WHEN has_availability = 't' THEN TRUE ELSE FALSE END AS has_availability,
        CAST(availability_30 AS INTEGER) AS availability_30,
        CAST(number_of_reviews AS INTEGER) AS number_of_reviews,
        
        -- Validating review scores, first casting them to NUMERIC
        CASE 
            WHEN CAST(review_scores_rating AS NUMERIC) >= 0 AND CAST(review_scores_rating AS NUMERIC) <= 100 THEN CAST(review_scores_rating AS FLOAT) 
            ELSE NULL  -- Nullify invalid scores
        END AS review_scores_rating,
        
        CASE 
            WHEN CAST(review_scores_accuracy AS NUMERIC) >= 0 AND CAST(review_scores_accuracy AS NUMERIC) <= 10 THEN CAST(review_scores_accuracy AS FLOAT)
            ELSE NULL
        END AS review_scores_accuracy,
        
        CASE 
            WHEN CAST(review_scores_cleanliness AS NUMERIC) >= 0 AND CAST(review_scores_cleanliness AS NUMERIC) <= 10 THEN CAST(review_scores_cleanliness AS FLOAT)
            ELSE NULL
        END AS review_scores_cleanliness,
        
        CASE 
            WHEN CAST(review_scores_checkin AS NUMERIC) >= 0 AND CAST(review_scores_checkin AS NUMERIC) <= 10 THEN CAST(review_scores_checkin AS FLOAT)
            ELSE NULL
        END AS review_scores_checkin,
        
        CASE 
            WHEN CAST(review_scores_communication AS NUMERIC) >= 0 AND CAST(review_scores_communication AS NUMERIC) <= 10 THEN CAST(review_scores_communication AS FLOAT)
            ELSE NULL
        END AS review_scores_communication,
        
        CASE 
            WHEN CAST(review_scores_value AS NUMERIC) >= 0 AND CAST(review_scores_value AS NUMERIC) <= 10 THEN CAST(review_scores_value AS FLOAT)
            ELSE NULL
        END AS review_scores_value
    FROM source
    WHERE listing_id IS NOT NULL  -- Ensure listing_id is not null as it's critical
)

SELECT * FROM cleaned