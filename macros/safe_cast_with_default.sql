{% macro safe_cast_with_default(column, target_type, default_value='NULL') %}
    -- Safely cast a column to target type with a default value on failure
    
    {% if target_type == 'INT64' %}
        COALESCE(SAFE_CAST({{ column }} AS INT64), {{ default_value }})
    {% elif target_type == 'FLOAT64' %}
        COALESCE(SAFE_CAST({{ column }} AS FLOAT64), {{ default_value }})
    {% elif target_type == 'STRING' %}
        COALESCE(SAFE_CAST({{ column }} AS STRING), {{ default_value }})
    {% elif target_type == 'BOOL' %}
        COALESCE(SAFE_CAST({{ column }} AS BOOL), {{ default_value }})
    {% elif target_type == 'DATE' %}
        COALESCE(SAFE_CAST({{ column }} AS DATE), {{ default_value }})
    {% elif target_type == 'DATETIME' %}
        COALESCE(SAFE_CAST({{ column }} AS DATETIME), {{ default_value }})
    {% elif target_type == 'TIMESTAMP' %}
        COALESCE(SAFE_CAST({{ column }} AS TIMESTAMP), {{ default_value }})
    {% else %}
        SAFE_CAST({{ column }} AS {{ target_type }})
    {% endif %}
{% endmacro %}

