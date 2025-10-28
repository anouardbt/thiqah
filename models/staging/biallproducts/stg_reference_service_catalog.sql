{{
    config(
        materialized='view',
        tags=['use_case_1', 'reference_data']
    )
}}

-- Reference data: Canonical service catalog

with source as (
    select * from {{ ref('reference_service_catalog') }}
),

standardized as (
    select
        -- Service identifiers
        upper(trim(service_code)) as service_code,
        canonical_service_name,
        service_category,
        description,
        
        -- Audit
        current_timestamp() as _loaded_at
        
    from source
)

select * from standardized

