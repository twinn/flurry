# Changelog

All notable changes to this project will be documented in this file.

This project adheres to [Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-04-10

### Added

- `use Flurry` macro and `@decorate batch(...)` decorator for declaring
  scatter-gather batched functions. The user defines a bulk (list-in,
  list-out) implementation; Flurry generates the single-item entry point
  that enqueues, blocks, and replies with the correlated result.
- GenStage-based producer/consumer pair per decorated function.
- Opportunistic mailbox-peek flushing: batches are emitted when either
  `batch_size` is reached or the producer's mailbox is empty. No timer-based
  flushing.
- Automatic input deduplication and key-based result correlation.
- Scalar (`returns: :one`, default) and grouped (`returns: :list`) return
  modes. Scalar mode raises `Flurry.AmbiguousBatchError` when the bulk
  function returns duplicate keys.
- Multi-argument batching with group-key partitioning.
- `batch_by:` option for normalizing non-batched arguments.
- `additive:` option for merging list-valued arguments across coalesced
  callers.
- `correlate:` option for custom key extraction from result records.
- `on_failure: :bisect | :fail_all` error handling strategies.
- `in_transaction: :warn | :safe | :bypass` transaction-aware modes.
- `overridable:` option on `use Flurry` for wrapping generated functions
  via `super/1`.
- `Flurry.Testing` module with bypass mode for use with
  `Ecto.Adapters.SQL.Sandbox`.
