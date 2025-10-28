{{
    config(
        materialized='table',
        tags=['use_case_3', 'halal', 'data_quality', 'dlq']
    )
}}

-- Dead Letter Queue (DLQ) for invalid JSON records
-- Captures records that fail JSON validation for investigation

with source as (
    select * from {{ ref('halal_transactions_raw') }}
),

invalid_json as (
    select
        transaction_id,
        merchant_name,
        transaction_data,
        created_at,
        
        -- Validation check
        case
            when transaction_data is null then 'NULL_JSON'
            when trim(transaction_data) = '' then 'EMPTY_JSON'
            when safe.parse_json(transaction_data) is null then 'INVALID_JSON_FORMAT'
            else 'VALID'
        end as error_type,
        
        -- Capture error details
        'JSON validation failed' as error_message,
        
        current_timestamp() as _detected_at
        
    from source
    where 
        transaction_data is null
        or trim(transaction_data) = ''
        or safe.parse_json(transaction_data) is null
)

select * from invalid_json

