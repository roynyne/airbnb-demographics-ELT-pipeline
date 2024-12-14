WITH source AS (
    SELECT * 
    FROM {{ ref('b_census_g01') }}
),

cleaned AS (
    SELECT
        -- Remove the "LGA" prefix and cast to integer
        CAST(REGEXP_REPLACE(lga_code_2016, '[^0-9]', '', 'g') AS INTEGER) AS lga_code,
        CAST(tot_p_m AS INTEGER) AS total_population_male,
        CAST(tot_p_f AS INTEGER) AS total_population_female,
        CAST(tot_p_p AS INTEGER) AS total_population_persons,
        CAST(count_psns_occ_priv_dwgs_p AS INTEGER) AS occupied_private_dwellings,
        CAST(count_persons_other_dwgs_p AS INTEGER) AS other_dwellings
    FROM source
    WHERE lga_code_2016 IS NOT NULL AND tot_p_p IS NOT NULL  -- Ensure critical fields are not null
)

SELECT * FROM cleaned