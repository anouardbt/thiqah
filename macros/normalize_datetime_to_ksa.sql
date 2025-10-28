{% macro normalize_datetime_to_ksa(datetime_column) %}
    -- Convert any datetime/timestamp to KSA timezone (Asia/Riyadh) and return as DATETIME
    -- Handles ISO timestamps, regular timestamps, and date strings
    
    CASE
        WHEN {{ datetime_column }} IS NULL THEN NULL
        -- Handle ISO 8601 format with Z (UTC)
        WHEN REGEXP_CONTAINS(CAST({{ datetime_column }} AS STRING), r'T.*Z$') THEN
            DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', CAST({{ datetime_column }} AS STRING)), '{{ var("timezone", "Asia/Riyadh") }}')
        -- Handle regular timestamp/datetime
        WHEN SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST({{ datetime_column }} AS STRING)) IS NOT NULL THEN
            DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST({{ datetime_column }} AS STRING)), '{{ var("timezone", "Asia/Riyadh") }}')
        -- Handle date only
        WHEN SAFE.PARSE_DATE('%Y-%m-%d', CAST({{ datetime_column }} AS STRING)) IS NOT NULL THEN
            DATETIME(PARSE_DATE('%Y-%m-%d', CAST({{ datetime_column }} AS STRING)))
        ELSE NULL
    END
{% endmacro %}

