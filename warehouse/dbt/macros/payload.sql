{#-
  payload() returns the VARIANT column that holds the Kafka event body.

  The Snowflake Kafka Sink Connector writes the decoded Avro record into a
  VARIANT column (default RECORD_CONTENT). Centralizing the name here means a
  change to connector config is a one-line var update, not an edit to every
  staging model.
-#}
{% macro payload() %}
    {{ var('payload_col', 'RECORD_CONTENT') }}
{% endmacro %}
