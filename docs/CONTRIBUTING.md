# How to Contribute to Debian Observer

This is a personal project.

Feel free to contribute issues or pull requests.
Just don't expect me to react promptly.

# RoadMap

What and how I plan to develop is documented in `docs/`.
It is linked in README.md too.

## How to

## Set Up a Development Environment

It is a good practice to separate the development environments from
each other and from your system.

## Option A: Python Venvs

If you prefer not to use containers, create a virtual environment and install
in editable mode. You will need to run NATS separately (see
`deploy/compose/docker-compose.yml`).

Create a virtual environment

```shell
python3 -m venv .your.venv
source .your.venv/bin/activate
pip install -e ".[dev]"
```

To tear it down without destroying it:
```shell
.your.venv/bin/deactivate
```

To re-enter your virtual environment:
`. .your.venv/bin/activate`


To destroy it:
```shell
rm -rf .your.venv
```

### Option B: Docker (Compose)

This repository provides a fully reproducible development environment
with NATS JetStream pre-configured.

**Services included:**

 - Python >=3.11 with project dependencies
 - NATS JetStream on `nats://nats:4222`
 - NATS CLI for stream inspection
 - Black, Ruff, MyPy pre-configured

Create a virtual environment

```shell
cd .devcontainer
docker compose up -d
```

To tear it down without destroying it:
```shell
docker stop debian-observer-dev
```

To re-enter your virtual environment:
```shell
cd .devcontainer
docker compose up -d
docker exec -ti debian-observer-dev bash
```

To destroy it:
```shell
cd .devcontainer
docker compose down
docker rmi  devcontainer-debian-observer-development
```

### Option C: VS Dev Container

The project includes a VS Code Dev Container configuration that
connects the docker development containers to VS Code.

**Requirements:**
- [VS Code](https://code.visualstudio.com/) + [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
- Docker + Docker Compose

**Quick start:**
1. Open the project folder in VS Code
2. Press `F1` → type "Dev Containers: Reopen in Container"
3. VS Code builds the container, installs dependencies, and starts NATS
4. Wait for the post-create setup to complete (installs `pip install -e ".[dev]"`)


## Verify

 - CLI: `debian-observer --help`
 - NATS: `nats server info`


# Testing

Run the test suite:

```bash
pytest
```

This runs all tests with coverage reporting. The minimum threshold is 80%.

In a freshly started development container the startup setup installs
pytest and its plugins in the background (`pip install -e ".[dev]"`); it
takes a few seconds. If `pytest` crashes right after container start,
wait a few seconds and run it again.

# Building/Packaging

```shell
python3 -m pip install --upgrade build
python3 -m build
```

Generates/refreshes:
```shell
ls -l dist/
total 88
-rw-r--r-- 1 root root 34413 oct  8 00:18 debian_observer-0.1.0-py3-none-any.whl
-rw-r--r-- 1 root root 52926 oct  8 00:18 debian_observer-0.1.0.tar.gz
```

# Distributing

`ln -sf debian_observer-0.1.0-py3-none-any.whl debian_observer.whl` facilitates
the distribution:


```shell
pipx install dist/debian_observer.whl
```


# Rules

## Feature Branches

Use an ad-hoc branch for each feature (a feature branch).

Feature branches are where development happens and show the real history,
but they are also ephemeral. Once the feature is complete,
it is refactored and merged/ported into `tdd` removing the glitches caused by

 - rebases, caused by a moving baseline,
 - back-and-forths due to discovery of edge cases or requirement misinterpretations,

Then, it is merged/ported into into `fdd`.
Then, it might be kept shortly for reference until it is finally removed.
Keeping too many branches is messy.

## Delivery Branches

No branch keeps the real history as it actually happened.

- `tdd` is near to the real history, with minor amends.
  It is an ideal branch for test-driven development (TDD).
  It holds a realistic sequence of minimum viable changes (MVCs).
  Each MVC is a (R)ed-(G)reen-(B)lue sequence:
  - R-commits add only new tests that don't pass. No application code.
  - G-commits add only add and/or modify application code to pass all the tests.
  - B-commits are not mandatory. They are pure refactoring commits.
    The purpose of B-commits might be real historic code improvements
    or purposeful intermediate steps for explanation of code changes.
    Several B commits might exist in (a BBB...) sequence.
- In `fdd` each commit holds a new complete feature. Each of them passes all its tests.
  Each commit in `fdd` is the squash of all its corresponding commits on `tdd`.

## Feature-Driven Development (FDD)

I started a Spec-Driven Development (SDD) experiment (the `sdd-spike` branch),
but it surfaced the classic flaws of old waterfall methods,
so I decided to fall back to FDD and TDD.

End users only care about working features, so we provide the `fdd` branch.

Definition of Done (DoD) for features includes:

 - tests,
 - application code,
 - documentation
 - and deployment artifacts.

## Test-Driven Development (TDD)

Tests first, and full coverage (with reasonable exceptions).

Prefer using Gherking-like test definitions and structures.


## Deployment: OpenTofu vs Docker Compose

Two deployment options are maintained for different use cases.

### OpenTofu (Production / Reproducible)

Use `deploy/opentofu/` when you need:

- **Declarative state management:** Infrastructure defined as code with `tofu plan` / `tofu apply`
- **Parameterized deployments:** Ports, image names, and network settings in `variables.tf`
- **Automated outputs:** URLs exposed as outputs (`nats_url`, `api_url`, `nats_monitoring_url`)
- **Full lifecycle:** `tofu init` → `plan` → `apply` → `destroy`
- **Build automation:** The observer image can be built automatically from `deploy/compose/Dockerfile`

**Trade-off:** Requires OpenTofu >= 1.6 installed on the host.

### Docker Compose (Development / Quick-start)

Use `deploy/compose/docker-compose.yml` when you need:

- **Zero additional tooling:** Only Docker Compose required
- **Fast iterative cycles:** `docker compose up -d` / `docker compose down`
- **Direct container access:** `docker exec -ti debian-observer bash`
- **Development convenience:** Mounts source code for live reloading

**Trade-off:** Imperative workflow (no state management), ports and settings hardcoded in YAML.

### Decision Matrix

| Need | OpenTofu | Docker Compose |
|:---|:---|:---|
| Production deployment | ✅ | ❌ |
| CI/CD reproducibility | ✅ | ❌ |
| Parameterized ports/settings | ✅ | ❌ |
| Local development | ⚠️ (overkill) | ✅ |
| Quick smoke test | ⚠️ (slower init) | ✅ |
| Live code reloading | ❌ | ✅ |

**Rule of thumb:** OpenTofu for anything that deploys to a shared environment; Compose for personal development on a single machine.

## Tiny commits

Use tiny commits. It helps to re-arrange them.

Commit texts must separate 3 concerns:

  1. Functional changes:
     - What can the user now do/expect that couldn't in the previous version.
     - What can the user no longer do/expect now that could in the previous version.
  2. Non-functional changes that are noticeable by the final users:
     (HOW the software does: speed, resources consumption, UX, cosmetics, ...)
  3. Internal changes noticeable only by the developers:
     (Readability, maintainability, reliability, testing coverage, dependencies, ...)

## Buildable state

 - The last commit on each PRs/feat branch must build and pass the CI/CD pipeline. 
 - On `tdd`, R-commits fail on purpose. G- and B- commits must all pass the CI/CD pipeline.
 - All commits on `fdd`*must pass the CI/CD pipeline.
