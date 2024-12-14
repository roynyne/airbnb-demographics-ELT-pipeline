{% snapshot lga_codes_snapshot %}

{{
    config(
        strategy='check',
        unique_key='lga_code',
        check_cols=['lga_name'],
        alias='lga_codes_snapshot'
    )
}}

SELECT
    lga_code,
    lga_name
FROM {{ ref('s_lga_codes') }}

{% endsnapshot %}