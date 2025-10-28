{% macro safe_json_extract(json_column, json_path, data_type='STRING') %}
    -- Safely extract value from JSON with error handling
    -- Returns NULL if JSON is invalid or path doesn't exist
    
    {% if data_type == 'STRING' %}
        JSON_EXTRACT_SCALAR({{ json_column }}, '{{ json_path }}')
    {% elif data_type == 'NUMERIC' %}
        SAFE_CAST(JSON_EXTRACT_SCALAR({{ json_column }}, '{{ json_path }}') AS NUMERIC)
    {% elif data_type == 'FLOAT64' %}
        SAFE_CAST(JSON_EXTRACT_SCALAR({{ json_column }}, '{{ json_path }}') AS FLOAT64)
    {% elif data_type == 'INT64' %}
        SAFE_CAST(JSON_EXTRACT_SCALAR({{ json_column }}, '{{ json_path }}') AS INT64)
    {% elif data_type == 'BOOL' %}
        SAFE_CAST(JSON_EXTRACT_SCALAR({{ json_column }}, '{{ json_path }}') AS BOOL)
    {% elif data_type == 'DATE' %}
        SAFE.PARSE_DATE('%Y-%m-%d', JSON_EXTRACT_SCALAR({{ json_column }}, '{{ json_path }}'))
    {% elif data_type == 'DATETIME' %}
        SAFE.PARSE_DATETIME('%Y-%m-%dT%H:%M:%S', JSON_EXTRACT_SCALAR({{ json_column }}, '{{ json_path }}'))
    {% else %}
        JSON_EXTRACT_SCALAR({{ json_column }}, '{{ json_path }}')
    {% endif %}
{% endmacro %}

