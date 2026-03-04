# 🏗️ BUILD.md — StreamSense AI
### Real-Time E-Commerce Analytics with Natural Language AI
#### DigitalOcean Gradient™ AI Hackathon Submission

---

## 🧠 What We're Building

**StreamSense AI** is a real-time e-commerce analytics platform that lets anyone — technical or not — ask questions about live business data in plain English and get instant, accurate answers.

The stack: **Go microservices** produce e-commerce events → **Apache Kafka** streams them in real-time → a **Go stream processor** enriches and validates data → **Snowflake** stores it in a layered warehouse → a **Text-to-SQL AI agent** hosted on **DigitalOcean Gradient™ AI** lets users query everything with natural language.

> "Why did revenue drop at 3pm?" → AI queries Snowflake → returns an actual answer.

---

## 🎯 Hackathon Alignment

| Requirement | How We Meet It |
|---|---|
| AI-powered application | LLM Text-to-SQL agent on Gradient AI answers natural language questions over live Snowflake data |
| Production-ready | Kafka + Schema Registry + DLQ + typed Go microservices |
| DigitalOcean Gradient™ AI | Model inference + GPU compute hosted entirely on Gradient AI |
| Full-stack | Producer → Kafka → Processor → Snowflake → AI Layer → Live Dashboard |
| Impactful | Makes data analytics accessible to non-technical business users |

---

## 🗂️ Project Structure

```
streamsense-ai/
├── producers/
│   ├── user-events/          # Go: simulates clickstream (views, searches, clicks)
│   ├── order-events/         # Go: simulates orders, payments, refunds
│   └── inventory-events/     # Go: simulates stock updates
├── processor/
│   └── stream-processor/     # Go: enriches, validates, routes to DLQ on failure
├── warehouse/
│   └── dbt/
│       ├── models/raw/       # Raw Kafka-landed events
│       ├── models/staging/   # Cleaned & typed tables
│       └── models/marts/     # fct_orders, dim_users, agg_revenue
├── ai-layer/
│   └── query-agent/          # Go: REST API wrapping LLM on Gradient AI (Text-to-SQL)
├── dashboard/
│   └── frontend/             # React: live metrics + natural language query UI
├── infra/
│   ├── docker-compose.yml    # Kafka, Zookeeper, Schema Registry, Kafka Connect
│   └── terraform/            # DigitalOcean + Snowflake provisioning
├── BUILD.md
└── README.md
```

---

## 🔧 Tech Stack

| Layer | Technology |
|---|---|
| Language | **Go 1.22** |
| Message Broker | **Apache Kafka** |
| Schema Management | **Confluent Schema Registry + Avro** |
| Stream Processing | **Go consumer groups** with stateful aggregation |
| Sink Connector | **Kafka Connect → Snowflake Sink Connector** |
| Data Warehouse | **Snowflake** |
| Transformations | **dbt Core** |
| AI / LLM Inference | **DigitalOcean Gradient™ AI** (GPU-backed, hosted LLM) |
| AI Pattern | **Text-to-SQL agent** with Snowflake schema context |
| Dashboard | **React + Recharts + TailwindCSS** |
| Infrastructure | **Docker Compose** (local) + **Terraform** (cloud) |
| Cloud | **DigitalOcean** |

---

## 🏛️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     GO PRODUCERS                        │
│  user-events  │  order-events  │  inventory-events      │
└───────────────────────┬─────────────────────────────────┘
                        │ Avro + Schema Registry
                        ▼
┌─────────────────────────────────────────────────────────┐
│                   APACHE KAFKA                          │
│  topic: user-events  │  order-events  │  inventory      │
│  topic: dlq (dead letter queue for bad messages)        │
└───────────┬──────────────────────────┬──────────────────┘
            │                          │
            ▼                          ▼
┌───────────────────┐      ┌───────────────────────────┐
│  GO STREAM        │      │  KAFKA CONNECT            │
│  PROCESSOR        │      │  Snowflake Sink Connector │
│  - Enrich events  │      └─────────────┬─────────────┘
│  - Validate       │                    │
│  - Aggregate      │                    │
└────────┬──────────┘                    │
         │ enriched topic                │ raw events
         ▼                              ▼
┌─────────────────────────────────────────────────────────┐
│                     SNOWFLAKE                           │
│  RAW layer → STAGING layer → MARTS layer (via dbt)      │
│  fct_orders │ dim_users │ agg_revenue_per_minute        │
└──────────────────────────┬──────────────────────────────┘
                           │
              ┌────────────┴─────────────┐
              │                          │
              ▼                          ▼
┌─────────────────────┐    ┌─────────────────────────────┐
│  AI QUERY AGENT     │    │  LIVE DASHBOARD             │
│  (Go REST API)      │    │  React + Recharts           │
│                     │    │  - Revenue per minute       │
│  "Why did revenue   │    │  - Conversion funnel        │
│   drop at 3pm?"     │    │  - Top products             │
│         │           │    │  - NL query interface       │
│         ▼           │    └─────────────────────────────┘
│  DigitalOcean       │
│  Gradient™ AI LLM   │
│  (Text-to-SQL)      │
│         │           │
│         ▼           │
│  Snowflake Query    │
│  → Answer to user   │
└─────────────────────┘
```

---

## 📦 Kafka Topics

| Topic | Producer | Consumer | Description |
|---|---|---|---|
| `user-events` | user-events service | stream-processor | Clicks, views, searches |
| `order-events` | order-events service | stream-processor | Orders, payments, refunds |
| `inventory-events` | inventory service | stream-processor | Stock updates |
| `enriched-events` | stream-processor | Kafka Connect | Joined & enriched events |
| `dlq` | stream-processor | Monitoring | Failed / malformed messages |

---

## ❄️ Snowflake Schema (dbt Layers)

### Raw Layer
- `raw.user_events` — raw Kafka landed events
- `raw.order_events`
- `raw.inventory_events`

### Staging Layer
- `staging.stg_users` — cleaned, typed, deduplicated
- `staging.stg_orders`
- `staging.stg_inventory`

### Mart Layer
- `marts.fct_orders` — order funnel facts
- `marts.dim_users` — user segments
- `marts.agg_revenue_per_minute` — real-time revenue aggregation
- `marts.fct_product_performance` — top products by views, purchases, revenue

---

## 🤖 AI Layer — Text-to-SQL Agent (Gradient AI)

The AI agent is a Go REST API that:

1. Accepts a natural language question from the dashboard
2. Builds a prompt containing the Snowflake schema context (mart tables + column descriptions)
3. Sends the prompt to the **LLM hosted on DigitalOcean Gradient™ AI**
4. LLM returns a SQL query
5. Go service executes the SQL against Snowflake
6. Returns the result + a natural language explanation back to the user

### Example Interaction
```
User: "What were our top 5 products by revenue in the last hour?"

Agent → Gradient AI LLM → generates SQL:
  SELECT product_name, SUM(revenue) as total_revenue
  FROM marts.fct_orders
  WHERE order_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
  GROUP BY product_name
  ORDER BY total_revenue DESC
  LIMIT 5;

Agent → executes on Snowflake → returns table + summary:
"In the last hour, your top product was Air Max 90 at $4,230 in revenue..."
```

---

## 🗓️ Build Plan

### Week 1 — Data Foundation
- [ ] Set up Kafka cluster with Docker Compose
- [ ] Configure Schema Registry + Avro schemas
- [ ] Build Go user-events producer
- [ ] Build Go order-events producer
- [ ] Build Go inventory-events producer
- [ ] Verify events flowing through Kafka topics

### Week 2 — Stream Processing & Warehouse
- [ ] Build Go stream processor (enrich, validate, DLQ routing)
- [ ] Set up Kafka Connect with Snowflake Sink Connector
- [ ] Design and create Snowflake raw layer tables
- [ ] Write dbt staging models
- [ ] Write dbt mart models (fct_orders, agg_revenue_per_minute)

### Week 3 — AI Layer
- [ ] Provision LLM on DigitalOcean Gradient™ AI
- [ ] Build Go query-agent REST API
- [ ] Write Text-to-SQL prompt with Snowflake schema context
- [ ] Test natural language → SQL → result pipeline
- [ ] Add natural language explanation of query results

### Week 4 — Dashboard & Polish
- [ ] Build React dashboard with live metrics
- [ ] Add natural language query input UI
- [ ] Connect dashboard to Go AI query agent
- [ ] Write Terraform for DigitalOcean provisioning
- [ ] Record demo video
- [ ] Write final README + architecture diagram

---

## 🚀 Running Locally

### Prerequisites
- Go 1.22+
- Docker & Docker Compose
- Snowflake account (free trial works)
- DigitalOcean account with Gradient AI access
- dbt Core installed

### Start Infrastructure
```bash
cd infra
docker-compose up -d
# Starts: Kafka, Zookeeper, Schema Registry, Kafka Connect
```

### Run Producers
```bash
cd producers/user-events && go run main.go
cd producers/order-events && go run main.go
cd producers/inventory-events && go run main.go
```

### Run Stream Processor
```bash
cd processor/stream-processor && go run main.go
```

### Run dbt Transformations
```bash
cd warehouse/dbt
dbt run
dbt test
```

### Run AI Query Agent
```bash
cd ai-layer/query-agent
export GRADIENT_AI_KEY=your_key
export SNOWFLAKE_DSN=your_dsn
go run main.go
```

### Run Dashboard
```bash
cd dashboard/frontend
npm install && npm run dev
```

---

## 🏆 Why This Wins

- **Gradient AI is central**, not cosmetic — the entire value of the product depends on it
- **Go is rare in DE hackathons** — it signals strong engineering fundamentals
- **Live demo is compelling** — judges can type questions and see real answers
- **Production patterns** throughout: Schema Registry, DLQ, layered warehouse, IaC
- **Real business value** — every e-commerce company needs this
