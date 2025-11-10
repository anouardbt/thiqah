{{
    config(
        materialized='incremental',
        unique_key='merchant_id, transaction_date_key',
        tags=['daily_metrics', 'incremental']
    )
}}

with daily_transactions as (
    select
        merchant_id,
        date(transaction_timestamp) as transaction_date_key,
        count(transaction_id) as total_transactions,
        avg(transaction_amount) as avg_transaction_value,
        sum(case when is_halal_certified then 1 else 0 end) / count(transaction_id) as halal_transaction_ratio,
        avg(days_until_certificate_expiry) as avg_days_until_certificate_expiry
    from {{ ref('fct_transactions') }}
    group by merchant_id, transaction_date_key
)

select * from daily_transactions