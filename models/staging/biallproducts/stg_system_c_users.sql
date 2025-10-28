{{
    config(
        materialized='view',
        tags=['use_case_1', 'multi_source_unification']
    )
}}

-- System C: Type coercion and normalization
-- Source has GUID user_id, status codes ACT/INA

with source as (
    select * from {{ ref('system_c_users') }}
),

normalized as (
    select
        -- Source metadata
        source_system,
        'system_c' as source_system_normalized,
        
        -- User identifier (GUID coerced to STRING)
        user_guid as user_id_raw,
        {{ generate_surrogate_key(['source_system', 'user_guid']) }} as user_key,
        
        -- Contact information (normalized)
        {{ normalize_email('user_email') }} as email,
        {{ normalize_phone_number('msisdn') }} as phone_number,
        
        -- Status (semantic mapping: ACT->active, INA->inactive)
        {{ normalize_status('status_code') }} as is_active,
        status_code as status_raw,
        
        -- Timestamps (ISO 8601 format with timezone)
        {{ normalize_datetime_to_ksa('created_timestamp') }} as created_at,
        {{ normalize_datetime_to_ksa('updated_timestamp') }} as last_updated,
        
        -- Audit
        current_timestamp() as _loaded_at
        
    from source
)

select * from normalized
