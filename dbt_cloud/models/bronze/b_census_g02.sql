{{
    config(
        unique_key='id',
        alias='censusg02'
    )
}}

select * from {{ source('raw', 'raw_census_g02') }}