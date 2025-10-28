{{
    config(
        materialized='view',
        tags=['use_case_1', 'multi_source_unification']
    )
}}

-- System B: Type coercion and normalization
-- Source has STRING user_id with 'U' prefix, boolean is_active

with source as (
    select * from {{ ref('system_b_users') }}
),

normalized as (
    select
        -- Source metadata
        source_system,
        'system_b' as source_system_normalized,
        
        -- User identifier (already STRING but inconsistent naming)
        userid as user_id_raw,
        {{ generate_surrogate_key(['source_system', 'userid']) }} as user_key,
        
        -- Contact information (normalized, mixed case in source)
        {{ normalize_email('email_address') }} as email,
        {{ normalize_phone_number('mobile') }} as phone_number,
        
        -- Status (already boolean but convert to standard format)
        {{ normalize_status('is_active') }} as is_active,
        cast(is_active as string) as status_raw,
        
        -- Timestamps (date only, convert to datetime)
        {{ normalize_datetime_to_ksa('registration_date') }} as created_at,
        {{ normalize_datetime_to_ksa('modified_date') }} as last_updated,
        
        -- Audit
        current_timestamp() as _loaded_at
        
    from source
)

select * from normalized

