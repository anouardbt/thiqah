{{
    config(
        materialized='table',
        tags=['use_case_2', 'mart', 'fact', 'payments']
    )
}}

-- Payment gateway bills fact table
-- Schema-enforced, analytics-ready billing data

with bills as (
    select * from {{ ref('stg_bills') }}
),

final as (
    select
        bill_id,
        billing_system_reference_no,
        customer_id,
        payment_date,
        amount,
        vat,
        total_amount,
        currency_code,
        payment_status,
        created_at,
        
        -- Calculated fields
        round(vat / amount * 100, 2) as vat_percentage,
        
        -- Date dimensions
        date(payment_date) as payment_date_key,
        extract(year from payment_date) as payment_year,
        extract(month from payment_date) as payment_month,
        extract(day from payment_date) as payment_day,
        extract(dayofweek from payment_date) as payment_day_of_week,
        
        -- Payment timing
        date_diff(date(payment_date), date(created_at), day) as days_to_payment,
        
        -- Status flags
        case when payment_status = 'paid' then 1 else 0 end as is_paid,
        case when payment_status = 'failed' then 1 else 0 end as is_failed,
        case when payment_status = 'pending' then 1 else 0 end as is_pending,
        
        -- Audit
        current_timestamp() as _dbt_loaded_at
        
    from bills
)

select * from final

