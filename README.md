# StreamSense AI 🚀

> Real-time e-commerce analytics powered by Apache Kafka, Go, Snowflake, and DigitalOcean Gradient AI.

**StreamSense AI** is a production-grade streaming analytics platform that monitors your e-commerce business second-by-second, detects anomalies before customers notice, explains *why* they happened using causal AI, and forecasts your revenue 30 minutes into the future.

---

## ✨ Key Features

- **🔍 Why Engine** — Automated causal root cause analysis. When revenue drops, the AI runs 6 parallel diagnostic checks and returns a root cause with confidence score in under 30 seconds (vs. the industry-standard 2–4 hours).
- **📈 Revenue Forecast Engine** — Linear regression over live streaming data projects 30-minute and end-of-day revenue with AI-generated confidence bands and intervention recommendations.
- **💬 Multi-Turn AI Query Agent** — Ask natural language questions about your store. The AI maintains conversation context across turns and generates Snowflake SQL automatically.
- **🔔 NL Alert Builder** — Type an alert in plain English ("alert me when Europe revenue drops 30%"). The AI compiles it into a live Kafka streaming rule, hot-loaded with zero downtime.
- **🌊 Live Event Feed** — Real-time SSE stream from Kafka topics to the dashboard — every order, page view, and inventory change appears instantly.
- **🏗️ Architecture Panel** — Live event counters per Kafka topic with animated data flow visualization.

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Event Streaming** | Apache Kafka + Confluent Schema Registry (Avro) |
| **Backend Services** | Go (Gin) — 7 microservices |
| **Data Warehouse** | Snowflake + dbt |
| **AI / LLM** | DigitalOcean Gradient AI (Llama 3.3 70B) |
| **Frontend** | React + Vite + Recharts + Tailwind CSS |
| **Infrastructure** | Docker Compose |

---

## 🏗️ Architecture

```
[Go Producers] ──Avro──▶ [Apache Kafka] ──▶ [Stream Processor]
   User Events                                      │
   Order Events          [Schema Registry]          │ SSE
   Inventory Events                                 ▼
                                             [React Dashboard]
[Anomaly Detector + Why Engine] ◀── rolling window ──┘
[Forecast Engine] ──────────────────SSE──────────────▶ Dashboard
[AI Query Agent] ◀── NL query ─── Dashboard
        │
        ▼
   [Snowflake / dbt]
```

---

## 📂 Project Structure

```
StreamSense/
├── producers/
│   ├── user-events/          # Go: clickstream producer
│   ├── order-events/         # Go: order & revenue producer
│   └── inventory-events/     # Go: stock level producer
├── processor/
│   └── stream-processor/     # Go: Kafka consumer + SSE bridge
├── ai-layer/
│   ├── query-agent/          # Go: multi-turn NL→SQL agent (Gradient AI)
│   ├── anomaly-detector/     # Go: Why Engine + NL Alert Builder
│   └── forecast-engine/      # Go: revenue forecast (linear regression)
├── dashboard/
│   └── frontend/             # React + Vite dashboard
├── warehouse/
│   └── dbt/                  # Snowflake staging + mart models
├── infra/
│   ├── docker-compose.yml    # Kafka, Zookeeper, Schema Registry, Kafka UI
│   └── schemas/              # Avro schemas for all event types
├── explain.txt               # Full technical explanation
└── README.md
```

---

## 🚀 Running Locally

### Prerequisites
- Docker + Docker Compose
- Go 1.22+
- Node.js + Bun
- (Optional) Snowflake account + DigitalOcean Gradient AI key

### 1. Start Infrastructure
```bash
cd infra && docker-compose up -d
```
Kafka UI available at `http://localhost:9000`

### 2. Start Backend Services
```bash
export SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX15.sdk
export GRADIENT_AI_KEY=your_key_here   # optional — uses fallbacks without it

# Producers
(cd producers/user-events && go run main.go &)
(cd producers/order-events && go run main.go &)
(cd producers/inventory-events && go run main.go &)

# Processor + AI Services
(cd processor/stream-processor && go run main.go &)
(cd ai-layer/query-agent && go run main.go &)
(cd ai-layer/anomaly-detector && go run main.go &)
(cd ai-layer/forecast-engine && go run main.go &)
```

### 3. Start Dashboard
```bash
cd dashboard/frontend && bun install && bun dev
```
Dashboard available at `http://localhost:8080`

---

## 🌐 Service Ports

| Service | Port |
|---|---|
| React Dashboard | `8080` |
| AI Query Agent | `8085` |
| Why Engine + Alert Builder | `8086` |
| Forecast Engine | `8087` |
| Stream Processor (SSE) | `8088` |
| Kafka Broker | `9092` |
| Schema Registry | `8081` |
| Kafka UI | `9000` |

---

## ⚙️ Environment Variables

| Variable | Service | Description |
|---|---|---|
| `GRADIENT_AI_KEY` | query-agent, anomaly-detector | DigitalOcean Gradient AI API key |
| `GRADIENT_AI_BASE_URL` | all AI services | AI inference endpoint (default: `https://inference.do-ai.run/v1`) |
| `GRADIENT_AI_MODEL` | all AI services | Model name (default: `meta-llama/Meta-Llama-3.3-70B-Instruct`) |
| `SNOWFLAKE_DSN` | query-agent | Snowflake DSN string for SQL execution |

---



## 📝 License

MIT
