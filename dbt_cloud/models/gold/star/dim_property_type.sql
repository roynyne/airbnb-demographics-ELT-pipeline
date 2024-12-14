WITH source AS (
    SELECT * FROM {{ ref('s_airbnb_listings') }}  -- Silver layer Airbnb listings
)

SELECT
    DISTINCT listing_id,  -- Add listing_id here for joining with fact_listings
    property_type,
    room_type,
    accommodates
FROM source
WHERE property_type IS NOT NULL