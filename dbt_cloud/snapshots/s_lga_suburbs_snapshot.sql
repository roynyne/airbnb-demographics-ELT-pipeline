{% snapshot lga_suburbs_snapshot %}

{{
    config(
        strategy='check',
        unique_key='suburb_name',
        check_cols=['lga_name'],
        alias='lga_suburbs_snapshot'
    )
}}

SELECT
    suburb_name,
    lga_name
FROM {{ ref('s_lga_suburbs') }}

{% endsnapshot %}