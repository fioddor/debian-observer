# How to Contribute to Debian Observer

This is a personal project.

Feel free to contribute issues or pull requests.
Just don't expect me to react promptly.

# RoadMap

What and how I plan to develop is documented in `docs/`.
It is linked in README.md too.

# Rules

## Branches

Use a branch for each feature.

 - `raw` is the real history as it actually happened.
 - `dev` shows the development (in a fantastic, ideal world).

## TDD

Tests first, and full coverage (with reasonable exceptions).

## Tiny commits

Use tiny commits. It helps to re-arrange them.

## Buildable state

 - For commits, balance the minimal size with buildable status (passing all tests).
 - The last commit on each PRs/feat branch **must** build and pass the CI/CD pipeline. 

