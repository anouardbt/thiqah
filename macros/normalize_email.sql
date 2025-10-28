{% macro normalize_email(email_column) %}
    -- Normalize email addresses: lowercase, trim whitespace
    
    CASE
        WHEN {{ email_column }} IS NULL THEN NULL
        WHEN TRIM({{ email_column }}) = '' THEN NULL
        WHEN NOT REGEXP_CONTAINS({{ email_column }}, r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$') THEN NULL
        ELSE LOWER(TRIM({{ email_column }}))
    END
{% endmacro %}

