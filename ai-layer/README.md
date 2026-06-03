# StreamSense AI Layer

Three Go services that turn live e-commerce data into AI-driven answers,
diagnostics, and forecasts. This document covers the applied-AI engineering
work: safety guardrails, the Text-to-SQL evaluation harness, prompt A/B
testing, and the diagnostic reasoning engine.

## Services

| Service | Port | Responsibility |
|---|---|---|
| `query-agent` | 8085 | Multi-turn natural-language → Snowflake SQL agent |
| `anomaly-detector` | 8086 | Why Engine (causal RCA) + NL alert compiler |
| `forecast-engine` | 8087 | Rolling-window linear-regression revenue forecast |

## 1. SQL Safety Guardrail (`query-agent/guard`)

LLM-generated SQL is never executed blindly. Every query passes through
`guard.ValidateSQL` before reaching Snowflake, which enforces:

- **Read-only**: must be a single `SELECT` / `WITH … SELECT`; any of
  `INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, MERGE, GRANT, …`
  is rejected as a whole-word match (so `updated_at` is fine, `UPDATE` is not).
- **No stacked statements**: comments are stripped and a second `;`-separated
  statement is refused (classic injection vector).
- **Schema allow-list**: tables referenced after `FROM`/`JOIN` must live in
  `marts.*` or `staging.*`, or be a CTE defined in the same query. Fails closed.

Run the tests:

```bash
cd ai-layer/query-agent
CGO_ENABLED=0 go test ./guard/...
```

## 2. Text-to-SQL Evaluation Harness (`query-agent/eval`)

A curated benchmark of hand-verified question → gold-SQL pairs, plus a grader
that scores generated SQL structurally (required tables, required
keywords/columns, and guardrail safety) rather than by brittle string equality.

```bash
cd ai-layer/query-agent

# Offline (deterministic, no API key needed) — great for CI and demos
CGO_ENABLED=0 go run ./eval/cmd/runeval -mode offline

# Live — calls the configured Gradient AI model
export GRADIENT_AI_KEY=...   # GRADIENT_AI_BASE_URL / GRADIENT_AI_MODEL optional
CGO_ENABLED=0 go run ./eval/cmd/runeval -mode live -json report.json
```

The harness exits non-zero if the best prompt variant scores below a 70% pass
rate, so it can gate CI.

### Prompt A/B testing

`eval.PromptVariants()` defines competing system prompts (`baseline` vs
`strict-rules`). The runner grades both and prints a comparison, letting you
measure which prompt produces more accurate, safer SQL before shipping it.

## 3. Why Engine — Diagnostic Reasoning (`anomaly-detector`)

When a revenue anomaly is detected, the Why Engine computes six operational
signals (payment success, order rate, cart abandonment, inventory coverage,
API latency, average order value) and asks the LLM to synthesize a causal
hypothesis with a confidence score and recommended action.

Signals are computed deterministically from a `MetricSnapshot` via explicit,
explainable thresholds (`diagnostics.go`) — not random numbers. A
`MetricsProvider` supplies the measurements; in production it is Snowflake,
and a `DerivedMetricsProvider` projects a coherent snapshot from the observed
anomaly when no warehouse is connected. If the LLM is unavailable,
`confidenceFromSignals` produces a fallback confidence from signal severity.

```bash
cd ai-layer/anomaly-detector
CGO_ENABLED=0 go test ./...
```

## Notes

- `CGO_ENABLED=0` is used for the pure-Go packages (guard, eval, diagnostics).
  The `query-agent` main package links the Snowflake driver, which needs CGO;
  set `SDKROOT` to a valid macOS SDK to build it locally.
- No secrets are required to run the offline eval or any unit tests.
