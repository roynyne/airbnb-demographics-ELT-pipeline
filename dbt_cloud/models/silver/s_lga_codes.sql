WITH source AS (
    SELECT * 
    FROM {{ ref('b_lga_codes') }}
),

cleaned AS (
    SELECT
        CAST(lga_code AS INTEGER) AS lga_code,
        TRIM(REPLACE(lga_name, ')', '')) AS lga_name  -- Remove any trailing parentheses from LGA names
    FROM source
    WHERE lga_code IS NOT NULL AND lga_name IS NOT NULL  -- Ensure no null values
)

SELECT * FROM cleaned