WITH source AS (
    SELECT * FROM {{ ref('s_airbnb_listings') }}  -- Silver layer Airbnb listings
)

SELECT
    DISTINCT listing_id,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates
FROM source
WHERE listing_id IS NOT NULL