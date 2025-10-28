{{
    config(
        materialized='view',
        tags=['use_case_1', 'identity_resolution', 'survivorship']
    )
}}

-- Apply survivorship rules to pick the "best" value for each attribute
-- Priority: source_priority_rank (higher is better), then freshness

with identity_clusters as (
    select * from {{ ref('int_user_identity_clusters') }}
),

-- Aggregate source systems per user first
source_systems_agg as (
    select
        global_user_id,
        string_agg(distinct source_system_normalized, ', ') as source_systems,
        count(*) as source_record_count
    from identity_clusters
    group by global_user_id
),

-- For each global user, pick the best record based on survivorship rules
best_record_per_user as (
    select
        ic.global_user_id,
        
        -- Pick email from highest priority source (latest if tie)
        first_value(ic.email) over (
            partition by ic.global_user_id
            order by 
                case when ic.email is not null then 1 else 0 end desc,
                ic.source_priority_rank desc,
                ic.last_updated desc
            rows between unbounded preceding and unbounded following
        ) as best_email,
        
        -- Pick phone from highest priority source (latest if tie)
        first_value(ic.phone_number) over (
            partition by ic.global_user_id
            order by 
                case when ic.phone_number is not null then 1 else 0 end desc,
                ic.source_priority_rank desc,
                ic.last_updated desc
            rows between unbounded preceding and unbounded following
        ) as best_phone_number,
        
        -- Pick is_active from highest priority source
        first_value(ic.is_active) over (
            partition by ic.global_user_id
            order by ic.source_priority_rank desc, ic.last_updated desc
            rows between unbounded preceding and unbounded following
        ) as best_is_active,
        
        -- Earliest creation date across all sources
        min(ic.created_at) over (
            partition by ic.global_user_id
        ) as earliest_created_at,
        
        -- Latest update across all sources
        max(ic.last_updated) over (
            partition by ic.global_user_id
        ) as latest_updated_at
        
    from identity_clusters ic
),

-- Deduplicate to one row per global user and join source systems
deduplicated as (
    select distinct
        b.global_user_id,
        b.best_email as email,
        b.best_phone_number as phone_number,
        b.best_is_active as is_active,
        b.earliest_created_at as created_at,
        b.latest_updated_at as last_updated,
        s.source_systems,
        s.source_record_count
        
    from best_record_per_user b
    left join source_systems_agg s
        on b.global_user_id = s.global_user_id
)

select * from deduplicated

