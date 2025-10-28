{{
    config(
        materialized='view',
        tags=['use_case_1', 'multi_source_unification']
    )
}}

-- System E: Services linked to users via email reference

with source as (
    select * from {{ ref('system_e_services') }}
),

normalized as (
    select
        -- Source metadata
        source_system,
        'system_e' as source_system_normalized,
        
        -- Service identifier
        svc_id as service_id,
        {{ generate_surrogate_key(['source_system', 'svc_id']) }} as service_key,
        
        -- Service details
        svc_description as service_name,
        category as service_code,
        
        -- User link (via email)
        {{ normalize_email('user_reference') }} as user_reference_email,
        null as user_id_raw,  -- No direct user_id in this system
        
        -- Provider information
        vendor as provider,
        
        -- Status normalization (Y/N flag)
        {{ normalize_status('active_flag') }} as is_active,
        active_flag as status_raw,
        
        -- Service period
        {{ normalize_datetime_to_ksa('activation_dt') }} as start_date,
        {{ normalize_datetime_to_ksa('expiry_dt') }} as end_date,
        
        -- Audit
        current_timestamp() as _loaded_at
        
    from source
)

select * from normalized

