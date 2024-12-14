WITH source AS (
    SELECT * 
    FROM {{ ref('b_census_g02') }}
),

cleaned AS (
    SELECT
        -- Remove the "LGA" prefix and cast to integer
        CAST(REGEXP_REPLACE(lga_code_2016, '[^0-9]', '', 'g') AS INTEGER) AS lga_code,
        CAST(median_age_persons AS INTEGER) AS median_age_persons,
        CAST(median_mortgage_repay_monthly AS INTEGER) AS median_mortgage_repay_monthly,
        CAST(median_tot_prsnl_inc_weekly AS INTEGER) AS median_tot_prsnl_inc_weekly,
        CAST(median_rent_weekly AS INTEGER) AS median_rent_weekly,
        CAST(median_tot_fam_inc_weekly AS INTEGER) AS median_tot_fam_inc_weekly,
        CAST(average_num_psns_per_bedroom AS FLOAT) AS average_num_psns_per_bedroom,
        CAST(median_tot_hhd_inc_weekly AS INTEGER) AS median_tot_hhd_inc_weekly,
        CAST(average_household_size AS FLOAT) AS average_household_size
    FROM source
    WHERE lga_code_2016 IS NOT NULL
)

SELECT * FROM cleaned
