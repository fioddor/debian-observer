# Debian Release Observatory

![Python](https://img.shields.io/badge/python-3.13-blue?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-blue?logo=fastapi&logoColor=white)
![NATS](https://img.shields.io/badge/NATS-2.16-blue?logo=jetstream&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-0.10-blue?logo=duckdb&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-darkviolet?logo=docker&logoColor=white)
![OpenTofu](https://img.shields.io/badge/OpenTofu-darkviolet?logo=opentofu&logoColor=white)

A reproducible and scalable observatory for studying
the evolution of Debian releases from public historical data.

Challenges:

  - **Flexibility**:
    - Data types: Working transparently, open source projects publish many
      kinds of data in various formats.
      The architecture and design of the analytic platform has to facilitate
      the processing of new datasource types.
    - Purposes: The provided data can help answer a lot of varied questions.
      This project is a platform. Each researcher will be able to plug-in
      targeted analyzer pipelines.
  - **Scalability**: Debian is old, big, and active. It has loads of data and
    is constantly generating more.
    This is a personal project, but I want it to be useful. So, it aims at
    enabling both individuals and organizations to analyze that data.
    Thus, it has to work at both sides of the spectrum: constrained in a small
    home lab and unrestricted in the cloud.

The platform is architected to reproducibly and scalably analyze data flows
in staged pipelines where compatible software pieces will process as input
the output produced by others.


# Using

See the [User Guide](docs/user-guide.md) and the [key flows](docs/design/key-flows.md).

For installation instructions see
[docs/user-guide.md#installation](docs/user-guide.md#installation).


# Contributing

This section is interesting too, if you are developing analytic plug-ins, even if they are private (not contributed).

Our technical [documentation](docs/) includes:

  - [How to contribute](docs/CONTRIBUTING.md)
  - evolving **product internals** documentation covering
    [architecture](docs/architecture/overview.md) (with
    [ADRs](docs/architecture/adr/)) and [design](docs/design/README.md)
    (data models, module contracts, etc.)
  - a phase-planned evolution **roadmap** as a collection of
    project documents per release
    ([R1](docs/release1.project.v0.4.md),
    [R2](docs/release2.project.v0.4.md),
    [R3](docs/release3.project.v0.1.md))
  - Software Bill of Materials ([SBOM](docs/SBOM.spdx.json)) in
    standard format.
