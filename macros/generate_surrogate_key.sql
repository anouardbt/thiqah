{% macro generate_surrogate_key(columns) %}
    -- Generate a deterministic surrogate key from multiple columns
    -- Uses MD5 hash for consistency
    
    TO_HEX(MD5(CONCAT(
        {% for column in columns %}
            COALESCE(CAST({{ column }} AS STRING), '')
            {% if not loop.last %}, '|', {% endif %}
        {% endfor %}
    )))
{% endmacro %}

