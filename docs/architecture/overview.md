# Architecture Overview

High-level system architecture for Debian Observer R1.

## Component Diagram

```mermaid
flowchart LR
    subgraph DataSources["Data Sources"]
        UDD[Ultimate Debian Database]
    end

    subgraph CoreSystem["Debian Observer R1"]
        direction LR
        Ingestion["Ingestion Pipeline"]
        Normalization["Normalization Layer"]
        Storage["Parquet Storage"]
        Messaging["NATS JetStream"]
        API["FastAPI Service"]
        Observability["OpenTelemetry"]
        CLI["CLI Tools"]
    end

    subgraph Consumers["Consumers"]
        Analyst[Analyst / Researcher]
        Dashboard[Monitoring / Dashboards]
    end

    UDD --> Ingestion
    Ingestion --> Normalization
    Normalization --> Storage
    Normalization --> Messaging
    Storage --> API
    Messaging --> API
    API --> Analyst
    Observability -.-> Ingestion
    Observability -.-> Normalization
    Observability -.-> Messaging
    Observability -.-> API
    CLI --> Ingestion
```

**Data flow:** ingestion → normalization → storage (Parquet) + messaging (NATS) → API

**Telemetry:** OpenTelemetry traces and metrics throughout all components.

## Architecture Decision Records

See `docs/architecture/adr/` for detailed ADRs.
