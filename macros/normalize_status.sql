{% macro normalize_status(status_column) %}
    -- Normalize various status representations to boolean
    -- Handles: active, enabled, 1, true, ACT, Y -> true
    -- Handles: inactive, disabled, 0, false, INA, N -> false
    
    CASE
        WHEN UPPER(TRIM(CAST({{ status_column }} AS STRING))) IN ('ACTIVE', 'ENABLED', '1', 'TRUE', 'ACT', 'Y', 'YES') THEN TRUE
        WHEN UPPER(TRIM(CAST({{ status_column }} AS STRING))) IN ('INACTIVE', 'DISABLED', '0', 'FALSE', 'INA', 'N', 'NO') THEN FALSE
        ELSE NULL
    END
{% endmacro %}

