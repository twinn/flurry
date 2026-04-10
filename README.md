# Flurry

[![CI](https://github.com/twinn/flurry/actions/workflows/ci.yml/badge.svg)](https://github.com/twinn/flurry/actions/workflows/ci.yml)
[![Hex.pm](https://img.shields.io/hexpm/v/flurry.svg)](https://hex.pm/packages/flurry)
[![Docs](https://img.shields.io/badge/docs-hexdocs-blue.svg)](https://hexdocs.pm/flurry)

Scatter-gather batching for Elixir, built on GenStage. Individual requests
are coalesced into a single bulk call and the results are correlated back
to each caller.

## Installation

Add `:flurry` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:flurry, "~> 0.1.0"}
  ]
end
```

## Overview

A module that `use`s `Flurry` defines a bulk (list-in, list-out) function
and decorates it with `@decorate batch(...)`. Flurry generates a
single-item entry point, runs a GenStage producer/consumer pipeline, and
correlates each caller's request with its result.

```elixir
defmodule MyApp.UserBatcher do
  use Flurry, repo: MyApp.Repo

  @decorate batch(get(id))
  def get_many(ids) do
    Repo.all(from u in User, where: u.id in ^ids)
  end
end
```

The decorator generates `MyApp.UserBatcher.get/1`. Under concurrency,
N simultaneous calls collapse into one `get_many/1` invocation:

```elixir
MyApp.UserBatcher.get(42)
#=> %User{id: 42, ...}

MyApp.UserBatcher.get(999)
#=> nil
```

## Flush Policy

A batch is emitted when any of the following conditions is met:

  * `batch_size` pending requests have accumulated, or
  * the producer's mailbox is empty, meaning no further requests are
    immediately queued, or
  * `max_wait` milliseconds have elapsed since the first pending request
    was enqueued.

The mailbox-empty check provides minimum latency under low load: a single
request arriving at an idle producer flushes immediately as a batch of one.
The `max_wait` timer (default 200ms) caps worst-case latency under slow
trickle conditions where requests arrive one at a time, fast enough to keep
the mailbox non-empty but too slowly to reach `batch_size`.

## Starting the Batcher

`use Flurry` generates `start_link/1` and `child_spec/1` on the module.
Add it to a supervision tree:

```elixir
children = [
  # ...
  MyApp.UserBatcher
]
```

## Options

### `batch_size`

Caps the size of any single bulk call. This is necessary because databases
such as PostgreSQL impose parameter limits on `WHERE id IN (...)` queries.

```elixir
# Module-wide default
children = [{MyApp.UserBatcher, batch_size: 500}]

# Per-decorated-function override
@decorate batch(get(id), batch_size: 500)
def get_many(ids), do: ...

@decorate batch(get_with_posts(id), batch_size: 50)
def get_many_with_posts(ids), do: ...
```

When more requests accumulate than `batch_size` allows, Flurry flushes
`batch_size` entries at a time across successive cycles.

### `max_wait:`

Maximum time in milliseconds that the first pending request waits before
the producer forces a flush. Defaults to `200`. Set to `nil` to disable.

```elixir
# Module-wide default
children = [{MyApp.UserBatcher, max_wait: 500}]

# Per-decorated-function override
@decorate batch(get(id), max_wait: 100)
def get_many(ids), do: ...
```

### `overridable:`

By default, the generated singular entry point is not overridable. Defining
a function with the same name and arity in the module produces a
redefinition error. The `overridable:` option on `use Flurry` allows
wrapping the generated function via `super/1`:

```elixir
defmodule MyApp.UserBatcher do
  use Flurry, repo: MyApp.Repo, overridable: [get: 1]

  @decorate batch(get(id))
  def get_many(ids) do
    Repo.all(from u in User, where: u.id in ^ids)
  end

  def get(id) do
    case super(id) do
      nil -> nil
      user -> %{user | display_name: "[#{user.id}] #{user.name}"}
    end
  end
end
```

The option accepts a keyword list of `name: arity` pairs. At compile time,
Flurry validates that every `:overridable` entry has a matching
`@decorate batch(...)` decoration with the same arity.

### `additive:`

Merges list-valued arguments across coalesced callers. This is useful when
different callers specify different values that the batch should combine,
such as Ecto preloads:

```elixir
@decorate batch(get(id, preloads), additive: [:preloads])
def get_many(ids, preloads) do
  Repo.all(from u in User, where: u.id in ^ids, preload: ^preloads)
end

# Three concurrent callers with distinct preloads:
MyApp.UserBatcher.get(1, [:posts])
MyApp.UserBatcher.get(2, [:comments])
MyApp.UserBatcher.get(3, [:posts, :profile])

# All three coalesce into one bulk call:
# get_many([1, 2, 3], [:posts, :comments, :profile])
```

Named arguments are excluded from the producer's routing key, so callers
that differ only on additive arguments share a batch. At flush time, the
additive values are merged using `list ++ list |> Enum.uniq/1`.

Restrictions:

  * Values at additive positions must be lists.
  * `additive:` and `batch_by:` cannot be combined on the same decoration.
  * Every name in `additive:` must appear in the decorator's group arguments.

### `batch_by:`

Normalizes the non-batched arguments for coalescing purposes. Accepts a
1-arity function that receives the raw tuple of non-batched arguments and
returns the canonical form used for both coalescing and the bulk function's
argument positions:

```elixir
@decorate batch(
  get_post(slug, user),
  batch_by: fn {user} -> {user.id} end
)
def get_many(slugs, user_id) do
  Repo.all(from p in Post, where: p.slug in ^slugs and p.user_id == ^user_id)
end
```

With `batch_by:`, the bulk function signature takes the normalized values,
not the raw decorator argument types. Valid values are closures and
captures. `batch_by:` on a single-arg decoration raises at compile time.

### `correlate:`

Specifies how the match key is extracted from each returned record. By
default, Flurry uses the first decorator argument's name as the record
field. Two forms are supported:

  * **Atom** -- names a top-level field on each returned record.
  * **Function** -- a 1-arity function that extracts the key from each
    record.

```elixir
@decorate batch(get(id), correlate: :uuid)
def get_many(ids) do
  Repo.all(from r in Row, where: r.uuid in ^ids)
end
```

### `timeout:`

Sets the `GenServer.call/3` timeout for the generated entry point.
Defaults to `5_000` (5 seconds).

```elixir
@decorate batch(get(id), timeout: 30_000)
def get_many(ids) do
  Repo.all(from u in User, where: u.id in ^ids, preload: [:posts, :comments])
end
```

### `returns: :one | :list`

Defaults to `:one`, where each caller's argument corresponds to at most one
returned record. Use `:list` when the bulk function returns multiple records
per key:

```elixir
@decorate batch(get_posts_by_user(user_id), returns: :list)
def get_many_posts_by_user(user_ids) do
  Repo.all(from p in Post, where: p.user_id in ^user_ids)
end
```

Using `:one` on a function that returns duplicate keys raises
`Flurry.AmbiguousBatchError`.

### `on_failure: :bisect | :fail_all`

Defaults to `:bisect`. When the bulk function raises or exits for a batch
of N entries, Flurry splits the batch in half and retries each half. The
recursion continues until a singleton failure isolates the problematic
entry. Every other caller in the original batch still receives their
correlated record.

Use `:fail_all` to surface a single failure as an error to every caller in
the batch without retrying.

> `:bisect` re-invokes the bulk function with smaller subsets of the same
> inputs. If the bulk function has non-idempotent side effects, use
> `on_failure: :fail_all` to avoid double-writes.

## Multi-Argument Batching

When the decorated function takes more than one argument, the first
argument is the batched variable and the remaining arguments determine
which callers share a batch. Callers whose non-batched arguments are
structurally equal coalesce into the same bulk call.

```elixir
@decorate batch(get_post(slug, user_id, active?))
def get_many_posts(slugs, user_id, active?) do
  Repo.all(
    from p in Post,
      where: p.slug in ^slugs and p.user_id == ^user_id and p.active == ^active?
  )
end
```

Each distinct combination of non-batched arguments has its own pending
list, `batch_size` cap, and slot in the producer's LRU flush rotation.

## Transactions

The bulk function runs in a background consumer process, not in the
caller's process. This has consequences for database transactions:

  1. **Writes** performed by the bulk function do not participate in the
     caller's transaction. If the caller's transaction rolls back, those
     writes are not rolled back.
  2. **Reads** performed by the bulk function do not see the caller's
     uncommitted writes.
  3. **Under `Ecto.Adapters.SQL.Sandbox`**, the consumer has no ancestry
     link to the test process, so queries produce ownership errors under
     `:manual` mode.

### `use Flurry, repo: ...`

The `repo:` option is mandatory. Pass the Ecto repo module, or `:none` for
batchers with no database involvement:

```elixir
use Flurry, repo: MyApp.Repo
use Flurry, repo: :none
```

### `in_transaction: :warn | :safe | :bypass`

Controls behavior when the generated entry point is called inside a
transaction. With a real repo the default is `:warn`; with `repo: :none`
the default is `:safe`.

  * **`:warn`** -- Logs a warning when `Repo.checked_out?/0` returns true.
  * **`:safe`** -- Suppresses the warning. Use for reads that do not require
    read-your-writes consistency.
  * **`:bypass`** -- Runs the bulk function inline in the caller's process
    when inside a transaction, so writes participate in the transaction's
    commit/rollback semantics. Outside a transaction, batches normally.

### Tests with Ecto SQL Sandbox

Enable global bypass in `test_helper.exs`:

```elixir
ExUnit.start()
Flurry.Testing.enable_bypass_globally()
```

With bypass enabled, every call to a decorated function runs the bulk
function inline in the caller's process. The batching pipeline is not
exercised in unit tests.

## Limitations

  * Calling the bulk function directly (e.g., `get_many/1`) runs in the
    caller's process and does not coalesce with concurrent singular callers.
  * The default additive merge function (`list ++ list |> Enum.uniq/1`)
    does not support nested preload trees.

## License

MIT -- see [LICENSE](LICENSE).
