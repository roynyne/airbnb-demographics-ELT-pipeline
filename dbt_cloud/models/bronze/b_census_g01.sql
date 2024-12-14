{{
    config(
        unique_key='id',
        alias='censusg01'
    )
}}

select * from {{ source('raw', 'raw_census_g01') }}