{{
    config(
        materialized='table',
        tags=['use_case_1', 'mart', 'fact']
    )
}}

-- Unified services fact table with user and service catalog relationships

with system_d_services as (
    select * from {{ ref('stg_system_d_services') }}
),

system_e_services as (
    select * from {{ ref('stg_system_e_services') }}
),

unified_users as (
    select * from {{ ref('dim_unified_users') }}
),

service_catalog as (
    select * from {{ ref('stg_reference_service_catalog') }}
),

-- Combine services from both systems
all_services as (
    select
        service_key,
        source_system_normalized,
        service_id,
        service_name,
        service_code,
        user_key,
        null as user_reference_email,
        provider,
        is_active,
        start_date,
        end_date,
        _loaded_at
    from system_d_services
    
    union all
    
    select
        service_key,
        source_system_normalized,
        service_id,
        service_name,
        service_code,
        null as user_key,
        user_reference_email,
        provider,
        is_active,
        start_date,
        end_date,
        _loaded_at
    from system_e_services
),

-- Join to unified users (by email for system E)
with_user_links as (
    select
        s.*,
        u.user_id as unified_user_id
        
    from all_services s
    left join unified_users u
        on s.user_reference_email = u.email
        or {{ generate_surrogate_key(['s.source_system_normalized', 's.user_key']) }} = 
           {{ generate_surrogate_key(['u.source_systems', 'u.user_id']) }}
),

-- Join to service catalog for canonical names
final as (
    select
        s.service_key,
        s.source_system_normalized,
        s.unified_user_id,
        s.service_id,
        s.service_name as service_name_raw,
        
        -- Reference data alignment
        coalesce(sc.canonical_service_name, s.service_name) as service_name_canonical,
        coalesce(sc.service_category, s.service_code) as service_category,
        sc.description as service_description,
        
        s.provider,
        s.is_active,
        s.start_date,
        s.end_date,
        
        -- Service status
        case
            when s.end_date is null then true
            when s.end_date > current_date() then true
            else false
        end as is_currently_active,
        
        -- Service duration
        case
            when s.end_date is null then date_diff(current_date(), date(s.start_date), day)
            else date_diff(date(s.end_date), date(s.start_date), day)
        end as service_duration_days,
        
        -- Audit
        current_timestamp() as _dbt_loaded_at
        
    from with_user_links s
    left join service_catalog sc
        on upper(trim(s.service_code)) = sc.service_code
)

select * from final

