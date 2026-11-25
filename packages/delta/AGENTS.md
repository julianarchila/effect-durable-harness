# @effect-durable-harness/delta

Immutable JSON revisions and op batches. Upstream license in `THIRD_PARTY_NOTICES.md`.

- **The op format is stable.** Documents are stored as these ops, so a change in the ops this package produces or accepts is a storage format change.
- **Linted like Effect's own library code.** `.oxlintrc.json` turns off the anti-slop rules here: this package validates `unknown`, uses `typeof` and `Proxy` traps. The tests also allow `no-extend-native`.
- **Errors are `Data.TaggedError`** and stay in process.
