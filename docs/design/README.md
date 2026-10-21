# Design Layer

High-level design for Debian Observer R1 — the layer *between* the
[architecture overview](../architecture/overview.md) and the code.

- [Data model](data-model.md) — entities and invariants
- [Module contracts](module-contracts.md) — what each module promises
- [Key flows](key-flows.md) — end-to-end flows across modules

## DRY rules

1. **Single source of truth.** Each fact lives in exactly one place;
   everything else links to it. Architecture owns the component map,
   ADRs own decisions, code owns the *how*. Design docs own only
   *intent* and *cross-module behaviour*.
2. **Never restate the code.** If a statement duplicates what a
   signature, docstring, or migration already says, delete it — it
   will rot at the first refactor.
3. **Docs move with code.** A design change touches the doc and its
   ADR in the same commit. A doc that lags loses authority and gets
   ignored.
4. **Cite the standard.** When a design statement rests on an external
   standard (Debian Policy, RFC, UDD schema…), link the source.
5. **A contract per top-level module.** New modules add their contract
   block in [module contracts](module-contracts.md); the coverage test
   discovers modules dynamically from `src/debian_observer/` and goes
   red until it exists — the gap self-detects.
