{{
    config(
        materialized='view',
        tags=['use_case_1', 'identity_resolution', 'deduplication']
    )
}}

-- Identity resolution: Group records that likely represent the same person
-- Based on matching email and/or phone number

with all_users as (
    select * from {{ ref('int_all_users_combined') }}
),

-- Create identity clusters based on email and phone
identity_groups as (
    select distinct
        email,
        phone_number,
        -- Generate a global identity key
        {{ generate_surrogate_key(['email', 'phone_number']) }} as global_user_id
        
    from all_users
    where email is not null or phone_number is not null
),

-- Join back to get all user records with their global ID
users_with_global_id as (
    select
        u.*,
        ig.global_user_id
        
    from all_users u
    left join identity_groups ig
        on (u.email = ig.email or u.phone_number = ig.phone_number)
)

select * from users_with_global_id

