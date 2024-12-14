{{
    config(
        unique_key='id',
        alias='lgacodes'
    )
}}

select * from {{ source('raw', 'raw_lga_codes') }}