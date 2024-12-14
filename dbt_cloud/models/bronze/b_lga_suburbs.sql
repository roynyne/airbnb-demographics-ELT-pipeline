{{
    config(
        unique_key='id',
        alias='lgasuburbs'
    )
}}

select * from {{ source('raw', 'raw_lga_suburbs') }}