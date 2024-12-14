WITH source AS (
    SELECT * FROM {{ ref('s_airbnb_listings') }}  -- Silver layer Airbnb listings
)

SELECT
    DISTINCT host_id,
    host_name,
    host_neighbourhood,
    host_since,
    host_is_superhost
FROM source
WHERE host_id IS NOT NULL