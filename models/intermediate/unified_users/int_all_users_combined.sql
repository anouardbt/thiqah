{{
    config(
        materialized='view',
        tags=['use_case_1', 'identity_resolution']
    )
}}

-- Combine all user sources into a single dataset
-- Prepare for deduplication and survivorship

with system_a as (
    select * from {{ ref('stg_system_a_users') }}
),

system_b as (
    select * from {{ ref('stg_system_b_users') }}
),

system_c as (
    select * from {{ ref('stg_system_c_users') }}
),

combined_users as (
    -- Union all user sources
    select * from system_a
    union all
    select * from system_b
    union all
    select * from system_c
),

with_priority as (
    select
        *,
        
        -- Source priority for survivorship (from dbt_project.yml vars)
        case source_system_normalized
            when 'system_a' then {{ var('source_priority', {}).get('system_a', 8) }}
            when 'system_b' then {{ var('source_priority', {}).get('system_b', 9) }}
            when 'system_c' then {{ var('source_priority', {}).get('system_c', 10) }}
            else 5
        end as source_priority_rank,
        
        -- Freshness rank (more recent = higher priority)
        row_number() over (
            partition by email, phone_number
            order by last_updated desc
        ) as freshness_rank
        
    from combined_users
    where email is not null or phone_number is not null
)

select * from with_priority

