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
  use Flurry, repo: MyApp.Repo

  @decorate batch(get(id))
  def get_many(ids) do
    Repo.all(from u in User, where: u.id in ^ids)
  end
end
```

`@decorate batch(get(id))` tells Flurry to generate a `get/1` entry
point whose single argument is `id`, and to correlate the returned
records back to callers by matching each record's `:id` field.

The `repo:` option is **mandatory** — it's the module Flurry calls
`checked_out?/0` on to detect transactional contexts. Pass `:none`
for batchers that have no database involvement (e.g. external API
coalescing). See [Transactions](#transactions) below for details.

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

### `overridable:` on `use Flurry` — wrap the generated function with `super/1`

By default, the generated singular entry point (`get/1` in the
examples above) isn't overridable — writing your own `def get/1` in
the same module produces a redefinition error. When you want to wrap
the generated function with extra logic, declare the name + arity on
`use Flurry`:

```elixir
defmodule MyApp.UserBatcher do
  use Flurry, repo: MyApp.Repo, overridable: [get: 1]

  @decorate batch(get(id))
  def get_many(ids) do
    Repo.all(from u in User, where: u.id in ^ids)
  end

  # Overrides the generated `get/1`. `super(id)` invokes the batched
  # implementation.
  def get(id) do
    case super(id) do
      nil -> nil
      user -> %{user | display_name: "[#{user.id}] #{user.name}"}
    end
  end
end
```

`overridable:` takes a keyword list of `name: arity` pairs. For each
entry, Flurry injects a delegate + `defoverridable` at the top of
your module *before* your function body is parsed — that's what lets
your subsequent `def get/1` become a legitimate override and reach
the batched implementation via `super/1`.

At compile time, Flurry validates that every `:overridable` entry has
a matching `@decorate batch(...)` decoration with the same arity.
Both of these raise `CompileError`:

```elixir
# No matching decoration:
use Flurry, repo: :none, overridable: [get_by_email: 1]
# (no @decorate batch(get_by_email(...)))

# Arity mismatch:
use Flurry, repo: :none, overridable: [get: 1]
@decorate batch(get(id, user_id))   # decorator declares arity 2
def get_many(ids, user_id), do: ...
```

Without `overridable:`, the generated function is a plain `def` — no
`defoverridable`, no super, and attempting to redefine it in the
module body is a compile error.

### `batch_by:` — normalize what defines a batch

For multi-arg decorations, the additional arguments beyond the
batched variable determine which callers share a batch. By default
those arguments are compared by Elixir's structural equality, which
can be too strict when they contain noise that shouldn't affect
batching — preloaded Ecto associations, extraneous metadata,
timestamp fields, etc.

`batch_by:` takes a 1-arity function that receives the raw tuple of
the non-batched arguments and returns the canonical form used for
both coalescing AND the bulk function's argument positions:

```elixir
@decorate batch(
  get_post(slug, user),
  batch_by: fn {user} -> {user.id} end
)
def get_many(slugs, user_id) do
  # Note: user_id is an integer here, post-normalization.
  Repo.all(from p in Post, where: p.slug in ^slugs and p.user_id == ^user_id)
end

# Call site stays ergonomic — pass the whole struct:
MyApp.PostBatcher.get_post("hello", current_user)
```

Two callers passing structurally-different-but-semantically-equal
structs (e.g. the same user with and without preloaded associations)
will coalesce into the same batch, and the bulk function sees just
the integer id — not the struct.

**The trade-off worth knowing:** with `batch_by:`, the bulk function
signature takes the *normalized* values, not the raw decorator arg
types. The caller site passes a struct; the bulk function receives
an integer. It's like a GraphQL resolver that works with parent
object ids: ergonomic at the call site, efficient at the
implementation. Document the normalized shape in the bulk function's
@doc if you expect others to read it.

Valid values:

- Closures: `batch_by: fn {a, b} -> {a.id, b} end`
- Captures: `batch_by: &MyApp.Utils.strip_preloads/1`

Not supported: MFA tuples, atoms (use `correlate:` for field-name
lookups), strings. `batch_by:` on a single-arg decoration raises at
compile time (there are no additional arguments to normalize).

### `correlate: :field_name`

By default, Flurry uses the first decorator argument's name as both the
generated parameter name *and* the record field to correlate by. Use
`correlate:` when the record's field name differs from the caller's arg
name:

```elixir
# Decorated arg is `id`, but records have a `:uuid` field.
@decorate batch(get(id), correlate: :uuid)
def get_many(ids) do
  Repo.all(from r in Row, where: r.uuid in ^ids)
end

MyApp.RowBatcher.get(42)   # => %{uuid: 42, ...}
```

The caller's `id` is matched against each returned record's `:uuid`
field. Useful when your app's naming (`id`) differs from your
underlying schema (`uuid`, `external_id`, etc.).

### `timeout: milliseconds`

The underlying `GenServer.call/3` timeout for the generated entry
point. Defaults to `5_000` (5 seconds). Override per decorator when
your bulk function's work dominates the wait — e.g. a batched query
over a large row set, or a remote API coalescer with its own
latency budget:

```elixir
@decorate batch(get(id), timeout: 30_000)
def get_many(ids) do
  Repo.all(from u in User, where: u.id in ^ids, preload: [:posts, :comments])
end
```

If the bulk function takes longer than `timeout`, the caller exits
with a `:timeout` reason — the same behavior as any `GenServer.call`
timeout.

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

## Multi-arg batching

If your decorated function takes more than one argument, the **first**
argument is the batched variable and the **remaining** arguments
select which callers share a batch. Callers whose non-batched
arguments are structurally equal coalesce into the same bulk call;
callers with different non-batched arguments run as independent
batches.

```elixir
@decorate batch(get_post(slug, user_id, active?))
def get_many_posts(slugs, user_id, active?) do
  Repo.all(
    from p in Post,
      where: p.slug in ^slugs and p.user_id == ^user_id and p.active == ^active?
  )
end

# These four calls produce THREE separate bulk invocations, one per
# distinct (user_id, active?) combination:
PostBatcher.get_post("a", 1, true)   # {1, true}
PostBatcher.get_post("b", 1, true)   # {1, true}  (coalesces with "a")
PostBatcher.get_post("c", 2, true)   # {2, true}  (own batch)
PostBatcher.get_post("d", 1, false)  # {1, false} (own batch)
```

Each distinct combination has its own pending list, its own
`batch_size` cap, its own priority queue for bisect retries, and its
own slot in the producer's LRU flush rotation so one combination
can't starve the others.

The decorator's first argument name is still the correlation field
(`slug` in the example above — each returned record must have a
`:slug` field matching the caller's request). To customize how the
non-batched arguments are compared — e.g. to strip preloaded Ecto
associations or ignore noise fields — see [`batch_by:`](#batch_by--normalize-what-defines-a-batch).

## Transactions

Flurry's bulk function runs in a background consumer process, not in
the caller's process. That has two concrete consequences for database
transactions that every user should understand — and the first one is
much more dangerous than the second.

1. **Writes made by the bulk function do NOT participate in the
   caller's transaction.** Because the consumer uses a different DB
   connection, any inserts/updates/deletes the bulk function performs
   are committed on the consumer's connection independently of the
   caller's transaction. If the caller's transaction later rolls
   back, those writes are **not rolled back** — committed state
   survives a "failed" transaction. This is a data integrity issue
   for any **mutation batcher** (insert, update, delete) and must be
   addressed by using `in_transaction: :bypass` on the decorator.
2. **Reads made by the bulk function do NOT see the caller's
   uncommitted writes.** If the caller has updated a row inside the
   transaction and then calls a batched read, the read will see the
   pre-update (committed) state, not the caller's in-progress update.
   This is a consistency issue (read-your-writes), less severe than
   the rollback problem but still worth knowing about. It usually
   doesn't matter for read batchers called before any writes in the
   same transaction.
3. **Under `Ecto.Adapters.SQL.Sandbox` in tests**, the sandbox routes
   queries by walking the calling process's `$callers`/`$ancestors`
   ancestry to find the test that checked out a connection. The
   consumer has no ancestry link to any test, so its queries hit
   ownership errors under `:manual` mode.

Flurry handles all three via the mandatory `repo:` option and a
per-decorator `in_transaction:` setting.

### `use Flurry, repo: ...` is mandatory

```elixir
use Flurry, repo: MyApp.Repo   # standard — uses Repo.checked_out?/0
use Flurry, repo: :none        # no DB involvement, skip transaction handling
```

Omitting `:repo` is a compile-time error. The explicit choice forces
you to think about transaction semantics instead of finding out in
production.

### `in_transaction: :warn | :safe | :bypass`

Three modes, set per decorator. With a real repo the default is
`:warn`; with `repo: :none` the default is `:safe` (and `:warn` /
`:bypass` are compile errors).

```elixir
# Default — warns at runtime if called inside a transaction, still batches
@decorate batch(get(id))
def get_many(ids), do: Repo.all(from u in User, where: u.id in ^ids)

# "I've reviewed this, batching inside transactions is fine" — no warning
@decorate batch(get(id), in_transaction: :safe)
def get_many(ids), do: Repo.all(from u in User, where: u.id in ^ids)

# Bypass — inside a transaction, runs the bulk fn inline in the caller's
# process so it participates in the transaction / rollback semantics.
# Outside a transaction, batches normally.
@decorate batch(insert(attrs), in_transaction: :bypass)
def insert_many(attrs_list), do: Repo.insert_all(User, attrs_list, returning: true)
```

**When to use each:**

- **`:bypass`** — **mandatory for any batcher that writes.** When
  called inside a transaction, Flurry skips the pipeline and runs
  the bulk function inline in the caller's process, so writes
  execute on the caller's connection and participate in the
  transaction's commit/rollback. Outside a transaction, batches
  normally. This is the only mode that is safe for mutation
  batchers. You lose batching for transactional calls, but you keep
  atomicity — which is why you chose to be in a transaction.
- **`:warn`** (default) — the driving signal is *"you didn't think
  about this; think about it now."* Every call logs a warning if
  `Repo.checked_out?/0` returns true. If this is a write batcher,
  the warning is telling you to switch to `:bypass`. If it's a read
  batcher and you've confirmed it doesn't need read-your-writes,
  switch to `:safe`.
- **`:safe`** — you've confirmed the batched call is a read that
  doesn't need read-your-writes consistency with any preceding
  writes in the same transaction. Typical for read batchers called
  early in a transaction body, or for reads of data the current
  transaction isn't mutating. **Never use `:safe` for a batcher
  that writes to the database** — your writes will escape the
  transaction and won't roll back.

### Tests with Ecto SQL Sandbox

Enable global bypass in your `test_helper.exs`:

```elixir
ExUnit.start()
Flurry.Testing.enable_bypass_globally()
```

With bypass enabled, every call to a decorated function runs the bulk
function inline in the caller's process — which, under the sandbox,
has ancestry back to the test and a properly checked-out connection.
`async: true` works out of the box.

The tradeoff: the batching pipeline itself isn't exercised in your
unit tests. The bulk function's own logic is still fully tested (it
runs with a singleton list), but producer/consumer behavior is not.
If you want integration tests of the pipeline, write them without
calling `enable_bypass_globally()` for that suite.

## Limitations

- **Correlation is by a single field atom.** The `correlate:` option
  picks which record field to match against the caller's argument,
  but it must be an atom naming a top-level field on the returned
  record. Computed keys, MFA callbacks, and anonymous functions are
  not supported today — if you need them, transform your bulk
  function's output to expose a matchable field before returning.
- **The bulk function bypasses the pipeline.** Calling your
  user-defined `get_many/1` (the plural, decorated function)
  directly runs in the caller's process with its own DB connection
  — it does not coalesce with concurrent singular callers. Callers
  with a list of ids in hand who want to participate in batching
  currently have to call `get/1` per element via `Task.async_stream`.
  Tracked in [issue #2](https://github.com/twinn/flurry/issues/2).
- **Additive argument merging isn't supported yet.** Arguments that
  should be unioned across coalesced callers (the motivating case is
  a `preloads:` list for Ecto queries — different callers may ask
  for different preloads and the batch should load their union) are
  tracked in [issue #1](https://github.com/twinn/flurry/issues/1).
  For now, either pre-compute the union at the call site or use
  distinct decorated functions per preload shape.

## License

MIT — see [LICENSE](LICENSE).
