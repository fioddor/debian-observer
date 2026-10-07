# How to Contribute to Debian Observer

This is a personal project.

Feel free to contribute issues or pull requests.
Just don't expect me to react promptly.

# RoadMap

What and how I plan to develop is documented in `docs/`.
It is linked in README.md too.

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
