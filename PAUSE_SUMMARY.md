# ⏸️ StreamSense AI - Pause Summary & Context

Work has been paused on the **StreamSense AI** project. All active services and simulators have been stopped to save resources.

## 🏗️ What Was Completed
1.  **Project Scaffolding**: Folder structure successfully set up according to `BUILD.md`.
2.  **Go Environment**: Go 1.26.0 installed and initialized for 5 microservices/producers.
3.  **Local Infrastructure (Docker)**: 
    *   Kafka, Zookeeper, Schema Registry, Kafka Connect, and Kafka UI configured.
    *   **Kafka UI** moved to port `9000` to avoid conflicts with the frontend.
4.  **Schema Definition**: Avro schemas created for `user_events`, `order_events`, and `inventory_events` in `infra/schemas/`.
5.  **Event Simulation**:
    *   **Producers**: High-performance Go producers written for all 3 data streams.
    *   **Verification**: Successfully verified that events are serialized via Avro and hit the Kafka broker.
6.  **Data Warehouse (Snowflake/dbt)**:
    *   `stg_user_events` and `stg_orders` dbt models written for the Snowflake staging layer.
    *   `agg_revenue_per_minute` mart model created for real-time dashboard visualization.
7.  **AI Layer**:
    *   `query-agent` (Go/Gin) scaffolded with system-prompt logic for Text-to-SQL.
8.  **Frontend**:
    *   `streamsense-ai-dashboard` repository cloned into `dashboard/frontend`.
    *   Dependencies fully installed using `bun`.

## 🚧 Status of Active Work
- [x] **Infrastructure Health**: All services pull and start correctly.
- [!] **Snowflake Sink Connector**: The JAR file for the Snowflake Kafka Connector (`v2.2.1`) needs to be redownloaded (previous 9B download was malformed).
- [ ] **AI Model Integration**: The `query-agent` currently uses a mock response. Actual LLM integration with Gradient AI is pending.

## 🚀 How to Resume
1.  **Restart Infrastructure**: `cd infra && docker-compose up -d`
2.  **Run Producers**:
    ```bash
    export SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX15.sdk
    (cd producers/user-events && go run main.go &)
    (cd producers/order-events && go run main.go &)
    (cd producers/inventory-events && go run main.go &)
    ```
3.  **Run Processor**: `(cd processor/stream-processor && go run main.go &)`
4.  **Start Dashboard**: `cd dashboard/frontend && bun dev`

---
*Created on 2026-03-02*
