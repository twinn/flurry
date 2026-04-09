# Flurry

[![CI](https://github.com/twinn/flurry/actions/workflows/ci.yml/badge.svg)](https://github.com/twinn/flurry/actions/workflows/ci.yml)

Scatter-gather batching for Elixir. A flurry of individual requests
coalesces into a single bulk call, then disperses back to the callers.

You write the plural function — one query, many ids — and decorate it.
Flurry generates the singular entry point, runs a GenStage pipeline
underneath, and correlates each caller's request with its result.

## Why

The classic N+1 problem: fifty LiveView mounts each call
`Users.get(user_id)` in the same instant, each firing its own
`SELECT * FROM users WHERE id = ?`. You wanted one query with
`WHERE id IN (?, ?, ..., ?)` instead. Flurry batches the fifty calls
into a single bulk query without the caller sites having to know.

Unlike most batching libraries, Flurry **has no flush timer**. Batches
are emitted when either:

  * `batch_size` pending requests have accumulated, or
  * the producer's mailbox is empty — i.e. there are no further
    requests immediately queued.

This gives maximum coalescing under bursts and minimum latency under
slow load: a singleton request arriving to an idle producer flushes
immediately as a batch of one.

## Installation

```elixir
def deps do
  [
    {:flurry, "~> 0.1.0"}
  ]
end
```

## Usage

### Declare a batched function

```elixir
defmodule MyApp.UserBatcher do
  use Flurry

  @decorate batch(get(id))
  def get_many(ids) do
    Repo.all(from u in User, where: u.id in ^ids)
  end
end
```

`@decorate batch(get(id))` tells Flurry to generate a `get/1` entry
point whose single argument is `id`, and to correlate the returned
records back to callers by matching each record's `:id` field.

### Start the batcher

`use Flurry` generates `start_link/1` and `child_spec/1` on your
module. Add it to your supervision tree:

```elixir
children = [
  # ...
  MyApp.UserBatcher
]
```

### Call it

```elixir
MyApp.UserBatcher.get(42)
#=> %User{id: 42, ...}

MyApp.UserBatcher.get(999)
#=> nil   # missing id
```

Under concurrency, N simultaneous calls collapse into one `get_many/1`
invocation:

```elixir
for id <- 1..50 do
  Task.async(fn -> MyApp.UserBatcher.get(id) end)
end
|> Enum.map(&Task.await/1)
# one bulk query, 50 records returned, 50 callers replied to.
```

## Options

### `batch_size`

Cap the size of any single bulk call. Necessary because PostgreSQL
and other databases have token/parameter limits on `WHERE id IN (?)`
queries.

```elixir
# Module-wide default
children = [{MyApp.UserBatcher, batch_size: 500}]

# Per-decorated-function override
@decorate batch(get(id), batch_size: 500)
def get_many(ids), do: ...

@decorate batch(get_with_posts(id), batch_size: 50)  # heavier rows
def get_many_with_posts(ids), do: ...
```

When more requests pile up than `batch_size` allows, Flurry flushes
`batch_size` at a time across successive cycles, respecting the cap
on every emission.

### `returns: :one | :list`

Default `:one` — each caller's argument corresponds to at most one
returned record, matched by the argument's name as a field. If your
bulk function legitimately returns many records per key, use
`:list`:

```elixir
@decorate batch(get_posts_by_user(user_id), returns: :list)
def get_many_posts_by_user(user_ids) do
  Repo.all(from p in Post, where: p.user_id in ^user_ids)
end

MyApp.PostBatcher.get_posts_by_user(42)
#=> [%Post{...}, %Post{...}, %Post{...}]
```

Using `:one` on a function that returns duplicate keys raises
`Flurry.AmbiguousBatchError` with a message pointing at the fix.

### `on_failure: :bisect | :fail_all`

**Default `:bisect`.** If the bulk function raises or exits for a
batch of N entries, Flurry splits the batch in half and retries
each half as its own event. The recursion descends to a singleton
failure, which isolates the bad entry — that one caller receives an
error, and every other caller in the original batch still gets their
correlated record.

```elixir
@decorate batch(get(id), on_failure: :bisect)  # explicit default
def get_many(ids), do: ...
```

Use `:fail_all` when you want a single failure to surface as an
error to every caller in the batch. No retry is attempted.

> **Idempotency warning.** `:bisect` re-invokes your bulk function
> with smaller subsets of the same inputs. If your bulk function has
> non-idempotent side effects — e.g. `Repo.insert_all/3` where some
> rows may have been inserted before the failure — **use
> `on_failure: :fail_all`** to avoid double-writes. Bisect is only
> safe for reads and other idempotent operations.

Errors delivered to callers:

  * **Raised exceptions** pass through raw: `{:error, %Postgrex.Error{...}}`,
    `{:error, %Ecto.Query.CastError{...}}`, your domain exceptions.
    You can pattern-match on their original type.
  * **Exits** (e.g. downstream `GenServer.call` timeouts) are wrapped
    in `Flurry.BulkCallFailed{kind: :exit, reason: reason}` so
    callers always see a struct.

## How correlation works

Given `@decorate batch(get(id))`, Flurry:

1. Reads the argument name (`id`) from the decorator call.
2. Uses it as both the parameter name of the generated `get/1` and
   the record field to correlate by.
3. Deduplicates caller arguments before calling your bulk function
   (so five callers for `id=7` result in a single entry in the
   passed list).
4. After the bulk function returns, builds a map
   `%{record.id => record}` and replies to each caller with the
   record matching their argument.

Missing records become `nil` in `:one` mode or `[]` in `:list`
mode.

## Multi-arg / group-keyed batching

If your decorated function takes more than one argument, the **first**
argument is the batched variable and the **remaining** arguments form
a *group key*. Callers sharing the same group key coalesce into the
same bulk call; callers with different group keys run as independent
batches.

```elixir
@decorate batch(get_post(slug, user_id, active?))
def get_many_posts(slugs, user_id, active?) do
  Repo.all(
    from p in Post,
      where: p.slug in ^slugs and p.user_id == ^user_id and p.active == ^active?
  )
end

# These three calls produce THREE separate bulk invocations, one per
# distinct (user_id, active?) tuple:
GroupedBatcher.get_post("a", 1, true)   # group {1, true}
GroupedBatcher.get_post("b", 1, true)   # group {1, true}  (coalesces with "a")
GroupedBatcher.get_post("c", 2, true)   # group {2, true}  (own batch)
GroupedBatcher.get_post("d", 1, false)  # group {1, false} (own batch)
```

Each group has its own pending list, its own `batch_size` cap, its
own priority queue for bisect retries, and its own slot in the
producer's LRU flush rotation (so no group can starve the others).

The decorator's first argument name is still the correlation field
(`slug` in the example above — each returned record must have a
`:slug` field matching the caller's request).

## Limitations

- **No arbitrary correlation functions.** The record field used for
  correlation is always the same atom as the first argument's name.
  If your record has a differently-named field, wrap your bulk
  function to rename the field before returning.
- **Group keys must be structurally comparable.** Tuples of atoms,
  numbers, and binaries work. Maps and structs are compared
  structurally by Elixir, which works but may surprise you with
  order-insensitive equality.
- **Single-node.** Flurry's GenStage pipeline runs in-process on one
  node; there is no cluster-aware coalescing.
- **Per-call timeout is a module-level default.** `GenServer.call`
  in `Flurry.Runtime.call/4` defaults to 5 seconds. If your bulk
  function is slow enough to risk timeouts, increase it via the
  producer config (or open an issue — we'll make it per-call).

## License

MIT — see [LICENSE](LICENSE).
