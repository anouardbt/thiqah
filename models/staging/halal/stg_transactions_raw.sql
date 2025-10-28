{{
    config(
        materialized='view',
        tags=['use_case_3', 'halal', 'json_flattening']
    )
}}

-- Halal DWH: JSON extraction and flattening
-- Validates JSON and extracts structured data

with source as (
    select * from {{ ref('halal_transactions_raw') }}
),

json_validated as (
    select
        transaction_id,
        merchant_name,
        transaction_data,
        created_at,
        
        -- Validate JSON
        case
            when transaction_data is null then false
            when trim(transaction_data) = '' then false
            when transaction_data = 'invalid_json_data_here' then false
            when safe.parse_json(transaction_data) is null then false
            else true
        end as is_valid_json,
        
        current_timestamp() as _loaded_at
        
    from source
),

json_extracted as (
    select
        transaction_id,
        merchant_name,
        created_at,
        is_valid_json,
        
        -- Extract merchant information
        {{ safe_json_extract('transaction_data', '$.merchant.id', 'STRING') }} as merchant_id,
        {{ safe_json_extract('transaction_data', '$.merchant.name', 'STRING') }} as merchant_name_from_json,
        {{ safe_json_extract('transaction_data', '$.merchant.category', 'STRING') }} as merchant_category,
        
        -- Extract transaction details
        {{ safe_json_extract('transaction_data', '$.transaction.amount.value', 'FLOAT64') }} as transaction_amount,
        {{ safe_json_extract('transaction_data', '$.transaction.amount.currency', 'STRING') }} as currency,
        {{ safe_json_extract('transaction_data', '$.transaction.timestamp', 'STRING') }} as transaction_timestamp_raw,
        {{ safe_json_extract('transaction_data', '$.transaction.payment_method', 'STRING') }} as payment_method,
        
        -- Extract certification information
        {{ safe_json_extract('transaction_data', '$.certification.halal_certified', 'BOOL') }} as is_halal_certified,
        {{ safe_json_extract('transaction_data', '$.certification.certificate_id', 'STRING') }} as certificate_id,
        {{ safe_json_extract('transaction_data', '$.certification.expiry_date', 'STRING') }} as certificate_expiry_raw,
        
        -- Keep original JSON for debugging
        transaction_data as json_raw,
        
        _loaded_at
        
    from json_validated
),

final as (
    select
        transaction_id,
        merchant_name,
        is_valid_json,
        
        -- Merchant
        merchant_id,
        coalesce(merchant_name_from_json, merchant_name) as merchant_name_resolved,
        merchant_category,
        
        -- Transaction
        transaction_amount,
        upper(currency) as currency_code,
        {{ normalize_datetime_to_ksa('transaction_timestamp_raw') }} as transaction_timestamp,
        payment_method,
        
        -- Certification
        coalesce(is_halal_certified, false) as is_halal_certified,
        certificate_id,
        safe.parse_date('%Y-%m-%d', certificate_expiry_raw) as certificate_expiry_date,
        
        -- Metadata
        created_at,
        json_raw,
        _loaded_at
        
    from json_extracted
)

select * from final

