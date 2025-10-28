{{
    config(
        materialized='table',
        tags=['use_case_1', 'mart', 'dimension']
    )
}}

-- Final unified user dimension
-- One row per unique user across all source systems

with unified_users as (
    select * from {{ ref('int_user_survivorship') }}
),

final as (
    select
        global_user_id as user_id,
        email,
        phone_number,
        is_active,
        created_at as first_seen_at,
        last_updated as last_seen_at,
        source_systems,
        source_record_count,
        
        -- Calculated fields
        date_diff(current_date(), date(last_updated), day) as days_since_last_update,
        
        -- Audit
        current_timestamp() as _dbt_loaded_at
        
    from unified_users
)

select * from final

