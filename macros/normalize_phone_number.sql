{% macro normalize_phone_number(phone_column) %}
    -- Normalize phone numbers to standard format: 966XXXXXXXXX (12 digits)
    -- Handles formats: +966-XX-XXX-XXXX, 966XXXXXXXXX, 05XXXXXXXX, etc.
    
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            CASE
                -- Remove all non-numeric characters
                WHEN {{ phone_column }} IS NULL THEN NULL
                WHEN TRIM(CAST({{ phone_column }} AS STRING)) = '' THEN NULL
                ELSE REGEXP_REPLACE(CAST({{ phone_column }} AS STRING), r'[^0-9]', '')
            END,
            -- If starts with 05, replace with 9665
            r'^05', '9665'
        ),
        -- If starts with 5 and length is 9, add 966 prefix
        r'^(5\d{8})$', '966\\1'
    )
{% endmacro %}

