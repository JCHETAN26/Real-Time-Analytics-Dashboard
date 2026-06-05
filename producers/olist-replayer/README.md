# Olist Dataset Replayer

Replays 100k real Brazilian e-commerce orders from the Olist public dataset
through the StreamSense Kafka pipeline at configurable speed.

## Setup

### 1. Download the dataset (free, ~45MB)

Go to https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
Click Download, unzip, and place these 4 files in `./data/`:

```
data/
  olist_orders_dataset.csv
  olist_order_items_dataset.csv
  olist_products_dataset.csv
  olist_customers_dataset.csv
```

---

## Option A — Direct Snowflake Load (recommended, no Kafka Connect needed)

Loads the Olist CSVs straight into Snowflake RAW tables using the Go driver.
This is the fastest path to getting real data into the warehouse.

### Prerequisites
- A Snowflake account (free trial at https://signup.snowflake.com)
- Run `infra/snowflake/01_setup.sql` once as ACCOUNTADMIN in a Snowflake worksheet

### Run

```bash
export SNOWFLAKE_DSN="myuser:mypassword@myaccount.snowflakecomputing.com/STREAMSENSE/RAW?warehouse=STREAMSENSE_WH"
export DATA_DIR=./data

cd snowflake-loader
go run main.go
```

Expected output:
```
📦 orders=99441  items_keys=98666  products=32951  customers=99441
🏗️  Built 112650 order rows and 99441 user event rows
✅ Snowflake connected
✅ Tables ready
⬆️  Loading 112650 rows into RAW.ORDER_EVENTS...
✅ ORDER_EVENTS loaded in 45s
⬆️  Loading 99441 rows into RAW.USER_EVENTS...
✅ USER_EVENTS loaded in 38s

🎉 LOAD COMPLETE
```

### Then run dbt

```bash
cd warehouse/dbt
dbt run
```

### Then start the AI query agent

```bash
export SNOWFLAKE_DSN="..."
export GRADIENT_AI_KEY="your_key"
cd ai-layer/query-agent
go run main.go
```

### Open the dashboard and ask real questions

```
http://localhost:8080

Try:
  "What are the top 5 product categories by revenue?"
  "Which month had the highest order count?"
  "What's the average order value for Electronics vs Fashion?"
  "How many orders were delivered vs cancelled?"
```

---

## Option B — Kafka Replay (streaming demo)

Replays events through Kafka topics in chronological order at configurable speed.
Requires Kafka Connect + Snowflake connector to be running for data to reach the warehouse.

### Prerequisites
- Kafka running: `cd infra && docker-compose up -d`

### Run

```bash
# 500x speed (default) — replays 2 years of data in ~2 hours
go run main.go

# Instant mode — fires all 211k events in ~8 seconds (for benchmarking)
SPEED_MULTIPLIER=0 go run main.go

# Custom settings
KAFKA_BROKER=localhost:9092 DATA_DIR=./data SPEED_MULTIPLIER=500 go run main.go
```

### What it produces

| Kafka topic    | Content                                                      |
|----------------|--------------------------------------------------------------|
| `order-events` | One event per order item — real price, category, status      |
| `user-events`  | One checkout event per order — real customer ID, device type |

Events are replayed chronologically using original purchase timestamps,
so time-series charts show real patterns (Black Friday spike Nov 2017, etc.).
