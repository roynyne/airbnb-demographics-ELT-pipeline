{{
    config(
        unique_key='id',
        alias='listings'
    )
}}

select * from {{ source('raw', 'raw_airbnb_listings') }}