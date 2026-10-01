# ADR-001 — Dataset Selection

Status: Proposed
Date: 2026-09-29

## Context

Debian is a large, continuously evolving software ecosystem with tens of thousands of
packages, dependencies, uploads, builds, bugs, migrations, and release constraints.
Understanding its evolution requires combining data from multiple sources
and observing how those facts change over time.

The Debian Ultimate Debian Database (UDD) consolidates data from various Debian services
into a single queryable database.
It provides public accessibility, sufficient historical depth, stable identifiers, and machine-readable structured data.
Alternative approaches like individual API scraping would introduce throttling, fragility, and inconsistency,
especially given the R1 time constraint of approximately two weeks.

A suitable data source must provide:

 - public accessibility without authentication barriers
 - sufficient historical depth for meaningful analysis
 - stable identifiers to track entities across time
 - machine-readable format to avoid unnecessary parsing complexity
 - reasonable acquisition cost (bandwidth, time, compute)
 - adequate volume to exercise the architecture meaningfully

## Decision

The primary data source for R1 will be the **Debian Ultimate Debian Database (UDD)**.

Each ingestion run will produce a dataset manifest recording the exact 
source, retrieval timestamp, source version, checksum, size, record count, and license.
This manifest becomes part of the provenance chain, enabling reproducibility.

If necessary during R1 or subsequent releases, additional public sources may be incorporated
(Debian archive metadata, snapshot data, migration information, build data, bug metadata),
but each source will be documented independently with its own manifest.

APIs requiring large numbers of throttled requests will not be the primary ingestion mechanism
when an equivalent bulk/public dataset exists.

## Consequences

 - UDD provides a single, consistent entry point for Debian package and release data
 - The dataset manifest ensures that any analysis can be traced back to the exact data snapshot used
 - The architecture will be validated against real-world data volume and heterogeneity
 - UDD may not contain all specialized datasets (e.g., detailed build logs); these remain available for future releases
 - Dependency on UDD's availability and structure; changes to UDD schema will require adaptation
