{% snapshot census_g02_snapshot %}

{{
    config(
        strategy='check',
        unique_key='lga_code',
        check_cols=[
            'median_age_persons', 'median_mortgage_repay_monthly', 
            'median_tot_prsnl_inc_weekly', 'median_rent_weekly', 
            'median_tot_fam_inc_weekly', 'average_num_psns_per_bedroom', 
            'median_tot_hhd_inc_weekly', 'average_household_size'
        ],
        alias='census_g02_snapshot'
    )
}}

SELECT
    lga_code,
    median_age_persons,
    median_mortgage_repay_monthly,
    median_tot_prsnl_inc_weekly,
    median_rent_weekly,
    median_tot_fam_inc_weekly,
    average_num_psns_per_bedroom,
    median_tot_hhd_inc_weekly,
    average_household_size
FROM {{ ref('s_census_g02') }}

{% endsnapshot %}
