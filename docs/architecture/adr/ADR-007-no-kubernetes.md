# ADR-007 — No Kubernetes in R1

Status: Proposed
Date: 2026-09-29


## Context

The goal is a reproducible local deployment that can later scale to cloud environments.

Kubernetes is the dominant container orchestration platform for distributed systems.
It provides service discovery, load balancing, self-healing, horizontal scaling, and declarative deployment.
For production-grade multi-node systems, Kubernetes is often the appropriate choice.

However, Kubernetes carries **significant operational complexity**:
cluster setup, networking configuration, storage classes, security policies, upgrades, and troubleshooting.
Learning curves and maintenance overhead are non-trivial, especially for resource-constrained environments.

The R1 system must run on modest hardware
(8 GB RAM development machine, 8 GB RAM primary server, additional 4 GB machines)
with requirement for no permanent cloud infrastructure.

R1 does not implement:

 - distributed graph computation
 - multiple specialized workers
 - complex stateful services
 - multi-node orchestration

The target deployment is deliberately small:
a single server or a few homelab machines, managed with reproducible configuration.


## Decision

**Kubernetes will not be used in R1.**

Container orchestration will be provided by **Docker Compose** for local deployment,
with **OpenTofu** for infrastructure-as-code where automation provides value.

The deployment must remain:

 - scalable in principle (documented cloud mappings)
 - reproducible and consistent
 - modular and isolated
 - dependency-managed

Cloud service mappings (e.g., AWS MSK for NATS, S3 for file storage)
will be documented as conceptual mappings without implying operational equivalence.
The local implementation will demonstrate declarative infrastructure, reproducibility, and configuration separation.

Kubernetes, GitOps, and full multi-node orchestration are explicitly deferred to future releases
when distributed workload requirements justify the operational investment.


## Consequences

Pros:

 - **Simpler** local deployment and debugging
 - **Reduced infrastructure learning curve** within the two-week timeline
 - OpenTofu enables **reproducible infrastructure definition**
 - Cloud mappings provide a **scalability path** without requiring immediate complexity
 - **Future** migration to Kubernetes is architecturally **planned**

Cons:
 - **No automatic horizontal scaling, self-healing, or rolling deployments** in R1
 - Future migration to Kubernetes is **not yet implemented**


Note: Docker Compose provides sufficient orchestration for single-node R1 services
