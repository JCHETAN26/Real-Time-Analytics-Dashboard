# StreamSense — Kafka → Snowflake Pipeline

End-to-end path from event producers to queryable warehouse marts.

```
Go producers ──Avro──▶ Kafka topics ──┬──▶ stream-processor ──SSE──▶ dashboard
                                       │         └─ bad records ─▶ dlq topic
                                       │
                                       └──▶ Kafka Connect (Snowflake Sink)
                                                     │ Snowpipe Streaming
                                                     ▼
                                       Snowflake RAW.* (VARIANT: RECORD_CONTENT)
                                                     │ dbt
                                                     ▼
                                       STAGING.*  ──▶  MARTS.*
```

## 1. Provision Snowflake (once)

```bash
# In a Snowflake worksheet, run as ACCOUNTADMIN:
infra/snowflake/01_setup.sql
```

Generate the connector's key pair and register the public key:

```bash
bash infra/snowflake/gen_keys.sh
# copy the printed single-line public key, then in Snowflake:
#   ALTER USER STREAMSENSE_CONNECTOR_USER SET RSA_PUBLIC_KEY = '<paste>';
```

## 2. Start infrastructure

```bash
cd infra && docker-compose up -d
```

The `connect` service mounts `infra/connect-plugins/`, which must contain the
Snowflake Kafka Connector JAR (`snowflake-kafka-connector-*.jar`). It is
gitignored due to size — download it from Confluent Hub if missing.

## 3. Register the Snowflake Sink connector

```bash
export SNOWFLAKE_ACCOUNT_URL=abc12345.us-east-1.snowflakecomputing.com
export SNOWFLAKE_PRIVATE_KEY="$(grep -v '^-----' infra/snowflake/keys/rsa_key.p8 | tr -d '\n')"
bash infra/connectors/register.sh
```

The connector lands each topic into a RAW table:

| Kafka topic | Snowflake table |
|---|---|
| `user-events` | `STREAMSENSE.RAW.user_events` |
| `order-events` | `STREAMSENSE.RAW.order_events` |
| `inventory-events` | `STREAMSENSE.RAW.inventory_events` |

With schematization disabled, each row stores the Avro body in a VARIANT
column named `RECORD_CONTENT`. The dbt staging models read this column via the
`payload()` macro (configurable through the `payload_col` var), so a connector
config change is a one-line edit, not a rewrite of every model.

## 4. Transform with dbt

```bash
cd warehouse/dbt
dbt run     # builds STAGING views and MARTS tables
dbt test
```

To validate the model graph without a warehouse connection:

```bash
python3 warehouse/dbt/validate_refs.py
```

## Dead Letter Queue

The stream processor routes any record it cannot deserialize or that fails
validation (e.g. negative revenue) to the `dlq` topic, attaching
`dlq_reason`, `dlq_detail`, `source_topic`, and `failed_at` headers. The
Snowflake connector is also configured with `errors.tolerance=all` and its own
`errors.deadletterqueue.topic.name=dlq`, so malformed records never block
ingestion on either side.
