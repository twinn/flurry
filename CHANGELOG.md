# Changelog

## Unreleased

Initial implementation.

- `use Flurry` macro + `@decorate batch(...)` decorator for declaring
  scatter-gather batched functions. User writes the bulk (list-in / list-out)
  implementation; Flurry generates the single-item entry point that enqueues,
  blocks, and replies with the correlated result.
- GenStage-based producer/consumer pair per decorated function.
- Opportunistic mailbox-peek flushing: flush when either `batch_size` is
  reached, or the producer's mailbox is empty — no arbitrary timers.
- Automatic input deduplication and key-based result correlation.
- Scalar (`returns: :one`, default) and grouped (`returns: :list`) modes.
  Scalar mode raises `Flurry.AmbiguousBatchError` if the bulk function
  returns duplicate keys.
