{{
    config(
        materialized='view',
        tags=['use_case_1', 'multi_source_unification']
    )
}}

-- System A: Type coercion and normalization
-- Source has INT user_id, status as 'active'/'disabled'

with source as (
    select * from {{ ref('system_a_users') }}
),

normalized as (
    select
        -- Source metadata
        source_system,
        'system_a' as source_system_normalized,
        
        -- User identifier (coerced to STRING for cross-system compatibility)
        cast(user_id as string) as user_id_raw,
        {{ generate_surrogate_key(['source_system', 'user_id']) }} as user_key,
        
        -- Contact information (normalized)
        {{ normalize_email('email') }} as email,
        {{ normalize_phone_number('phone_number') }} as phone_number,
        
        -- Status (semantic mapping to boolean)
        {{ normalize_status('status') }} as is_active,
        status as status_raw,
        
        -- Timestamps (normalized to KSA timezone)
        {{ normalize_datetime_to_ksa('created_at') }} as created_at,
        {{ normalize_datetime_to_ksa('last_updated') }} as last_updated,
        
        -- Audit
        current_timestamp() as _loaded_at
        
    from source
)

select * from normalized
