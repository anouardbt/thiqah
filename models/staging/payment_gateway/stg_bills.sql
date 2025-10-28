{{
    config(
        materialized='view',
        tags=['use_case_2', 'payment_gateway', 'schema_enforcement']
    )
}}

-- Payment Gateway: Strict schema enforcement and type safety
-- Ensures correct types to prevent Parquet auto-inference issues

with source as (
    select * from {{ ref('payment_gateway_bills') }}
),

enforced_schema as (
    select
        -- Bill identifier - strict INT64, reject if non-numeric
        {{ safe_cast_with_default('bill_id', 'INT64') }} as bill_id,
        
        -- Reference number - MUST remain STRING (no numeric coercion)
        cast(billing_system_reference_no as string) as billing_system_reference_no,
        
        -- Customer identifier
        cast(customer_id as string) as customer_id,
        
        -- Payment date - convert to DATETIME in KSA timezone, strip TZ
        {{ normalize_datetime_to_ksa('payment_date') }} as payment_date,
        
        -- Amounts - keep as FLOAT64, preserve decimals
        cast(amount as float64) as amount,
        cast(vat as float64) as vat,
        cast(total_amount as float64) as total_amount,
        
        -- Currency code
        upper(trim(currency)) as currency_code,
        
        -- Status
        lower(trim(status)) as payment_status,
        
        -- Created timestamp
        {{ normalize_datetime_to_ksa('created_at') }} as created_at,
        
        -- Data quality flags
        case
            when bill_id is null then false
            when billing_system_reference_no is null then false
            when amount is null or vat is null then false
            when payment_date is null then false
            else true
        end as is_valid_record,
        
        -- Audit
        current_timestamp() as _loaded_at
        
    from source
)

select * from enforced_schema
where is_valid_record = true  -- Filter out invalid records
