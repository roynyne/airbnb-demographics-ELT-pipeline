{% snapshot census_g01_snapshot %}

{{
    config(
        strategy='check',
        unique_key='lga_code',
        check_cols=['total_population_male', 'total_population_female', 'total_population_persons', 'occupied_private_dwellings', 'other_dwellings'],
        alias='census_g01_snapshot'
    )
}}

SELECT
    lga_code,
    total_population_male,
    total_population_female,
    total_population_persons,
    occupied_private_dwellings,
    other_dwellings
FROM {{ ref('s_census_g01') }}

{% endsnapshot %}