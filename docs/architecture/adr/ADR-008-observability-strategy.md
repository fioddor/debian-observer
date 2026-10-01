# ADR-008 — Observability Strategy

Status: Proposed
Date: 2026-09-29


## Context

Observability is critical for understanding system behavior, debugging failures, optimizing performance, and planning capacity.
Without telemetry, the system operates as a black box, making incidents difficult to diagnose and improvements difficult to measure.

However, a complete observability platform (Prometheus, Grafana, Loki, Tempo, Alertmanager) represents significant operational complexity:
deployment, configuration, storage management, dashboard creation, and alert tuning.
For R1's two-week timeline and resource-constrained environment, a full platform would be overkill.

The project must balance two needs:

 1. producing portfolio-quality evidence of system behavior for demonstration and debugging
 2. avoiding operational overhead that distracts from core data platform goals

R2 and R3 will likely require distributed tracing, metrics aggregation, and log correlation across multiple services.
Establishing the right instrumentation model in R1 prevents retrofitting pain later.


## Decision

**OpenTelemetry** will be introduced from R1 as the consistent instrumentation model.

The system will emit:
 - traces representing pipeline execution paths (ingestion → normalization → event publication → consumer → Parquet write)
 - metrics for core operations (ingestion duration, records ingested, events published/processed/failed, consumer lag, query duration)
 - structured logs where appropriate

A lightweight local backend may be used during development for immediate visibility.

The following are explicitly **deferred** to future releases:

 - Prometheus/Grafana metrics platform
 - Loki log aggregation
 - Tempo distributed tracing backend
 - SLOs and error budgets
 - Automated alerting and remediation
 - Chaos testing

OpenTelemetry was chosen because:

 - it provides a **vendor-neutral** instrumentation standard
 - it supports traces, metrics, and logs through a **unified API**
 - it creates a **migration path to full backends** in future releases
 - it is the industry **standard** for cloud-native observability

## Consequences

Pros

 - Instrumentation is **present from day one**, not retrofitted later
 - Core metrics **enable quantitative measurement** of R1 performance and reliability
 - **Traces** demonstrate pipeline execution flow
 - **Local lightweight backends** suffice for R1; production-grade platforms belong to R2/R3

Cons:
 - OpenTelemetry introduces a **dependency** and modest configuration **overhead**
 - Without a complete backend, **long-term metric retention and advanced visualization are unavailable** in R1
