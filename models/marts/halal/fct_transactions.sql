{{
    config(
        materialized='table',
        tags=['use_case_3', 'mart', 'fact', 'halal']
    )
}}

-- Halal certification transactions fact table
-- JSON flattened into relational structure

with transactions as (
    select * from {{ ref('stg_transactions_raw') }}
    where is_valid_json = true
),

final as (
    select
        transaction_id,
        
        -- Merchant dimension
        merchant_id,
        merchant_name_resolved as merchant_name,
        merchant_category,
        
        -- Transaction details
        transaction_amount,
        currency_code,
        transaction_timestamp,
        payment_method,
        
        -- Certification
        is_halal_certified,
        certificate_id,
        certificate_expiry_date,
        
        -- Certificate status
        case
            when certificate_expiry_date is null then null
            when certificate_expiry_date < current_date() then false
            else true
        end as is_certificate_valid,
        
        case
            when certificate_expiry_date is not null then
                date_diff(certificate_expiry_date, current_date(), day)
            else null
        end as days_until_certificate_expiry,
        
        -- Date dimensions
        date(transaction_timestamp) as transaction_date_key,
        extract(year from transaction_timestamp) as transaction_year,
        extract(month from transaction_timestamp) as transaction_month,
        extract(day from transaction_timestamp) as transaction_day,
        extract(hour from transaction_timestamp) as transaction_hour,
        
        -- Metadata
        created_at,
        
        -- Audit
        current_timestamp() as _dbt_loaded_at
        
    from transactions
)

select * from final

