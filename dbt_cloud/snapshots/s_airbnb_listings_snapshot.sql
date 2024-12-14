{% snapshot airbnb_listings_snapshot %}

{{
    config(
        strategy='check',
        unique_key='listing_id',
        check_cols=['price', 'availability_30', 'number_of_reviews', 'review_scores_rating'],
        alias='airbnb_listings_snapshot'
    )
}}

SELECT
    listing_id,
    price,
    availability_30,
    number_of_reviews,
    review_scores_rating
FROM {{ ref('s_airbnb_listings') }}

{% endsnapshot %}