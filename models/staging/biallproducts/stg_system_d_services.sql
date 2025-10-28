{{
    config(
        materialized='view',
        tags=['use_case_1', 'multi_source_unification']
    )
}}

-- System D: Services linked to users via user_id

with source as (
    select * from {{ ref('system_d_services') }}
),

normalized as (
    select
        -- Source metadata
        source_system,
        'system_d' as source_system_normalized,
        
        -- Service identifier
        cast(service_id as string) as service_id,
        {{ generate_surrogate_key(['source_system', 'service_id']) }} as service_key,
        
        -- Service details
        service_name,
        service_code,
        
        -- User link (mixed types in source)
        cast(user_id as string) as user_id_raw,
        {{ generate_surrogate_key(['source_system', 'user_id']) }} as user_key,
        
        -- Provider information
        provider,
        
        -- Status normalization
        {{ normalize_status('status') }} as is_active,
        status as status_raw,
        
        -- Service period
        {{ normalize_datetime_to_ksa('start_date') }} as start_date,
        {{ normalize_datetime_to_ksa('end_date') }} as end_date,
        
        -- Audit
        current_timestamp() as _loaded_at
        
    from source
)

select * from normalized

