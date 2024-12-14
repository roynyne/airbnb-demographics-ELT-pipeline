WITH source AS (
    SELECT * FROM {{ ref('s_lga_suburbs') }}  -- Silver layer suburb and LGA mapping
)

SELECT
    DISTINCT lga_name,
    suburb_name
FROM source
WHERE lga_name IS NOT NULL