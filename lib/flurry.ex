defmodule Flurry do
  @moduledoc """
  Scatter-gather batching for Elixir. A flurry of individual requests
  coalesces into a single bulk call, then disperses back to the callers.

  ## Usage

      defmodule MyApp.UserBatcher do
        use Flurry

        @decorate batch(get(id))
        def get_many(ids) do
          Repo.all(from u in User, where: u.id in ^ids)
        end
      end

  The decorator generates `MyApp.UserBatcher.get/1` for you. Callers do:

      MyApp.UserBatcher.get(42)
      #=> %User{id: 42, ...}

  Under the hood, `get/1` enqueues the request into a GenStage producer.
  A consumer pulls a batch, invokes your `get_many/1` with a de-duplicated
  list of ids, correlates the returned records back to each caller by the
  `:id` field, and replies.

  ## Flush policy

  Flurry does not use a flush timer. A batch is emitted when either:

    * `batch_size` (default 100) pending requests have accumulated, or
    * the producer's mailbox is empty — i.e. there are no further requests
      waiting to be enqueued *right now*.

  This gives maximum batching under bursts and minimum latency under slow
  load: singleton requests that arrive to an empty producer flush
  immediately.

  ## Error handling

  The default `:on_failure` strategy is `:bisect`: if the bulk function
  raises or exits for a batch of N entries, Flurry splits the batch in
  half and retries each half as its own event. Splits recurse until a
  singleton failure isolates the bad entry, whose caller receives an
  error. Every other caller in the original batch still gets their
  correlated record.

  The alternative is `on_failure: :fail_all`, where a single failure
  delivers the error to every caller in the batch and no retry is
  attempted.

  > **Idempotency warning.** `:bisect` re-invokes your bulk function with
  > smaller subsets of the same inputs. If your bulk function has
  > non-idempotent side effects — e.g. `Repo.insert_all/3` where some
  > rows may have been inserted before the failure — **use
  > `on_failure: :fail_all`** to avoid double-writes. Bisect is only safe
  > for reads and other idempotent operations.

  Raised exceptions pass through to callers as `{:error, exception}`,
  preserving their original type so you can match on `Postgrex.Error`,
  `Ecto.Query.CastError`, etc. Exits are wrapped in
  `Flurry.BulkCallFailed` so callers see a uniform struct.

  ## Return modes

  The default is `returns: :one` — each caller's argument is expected to
  match at most one record, keyed by the argument's name. If your bulk
  function can legitimately return multiple records per caller, declare it
  with `returns: :list`:

      @decorate batch(get_posts_by_user(user_id), returns: :list)
      def get_posts_for_users(user_ids) do
        Repo.all(from p in Post, where: p.user_id in ^user_ids)
      end

  In `:list` mode, `get_posts_by_user/1` returns a list of records per
  caller (possibly empty).

  Using `returns: :one` on a function that returns duplicate keys raises
  `Flurry.AmbiguousBatchError` with a clear message pointing at the fix.

  ## Starting the batcher

  `use Flurry` generates `start_link/1` and `child_spec/1` on your module,
  so you add it to your supervision tree the normal way:

      children = [
        MyApp.UserBatcher
      ]

  ## Options

  Pass options through `child_spec` / `start_link`:

    * `:batch_size` — maximum number of requests in a single bulk call
      (default `100`).
  """

  @doc false
  defmacro __using__(_opts) do
    quote do
      use Flurry.Decorators

      Module.register_attribute(__MODULE__, :flurry_batches, accumulate: true)
      @before_compile Flurry
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    batches = env.module |> Module.get_attribute(:flurry_batches) |> Enum.reverse()

    singular_defs =
      Enum.map(batches, fn batch ->
        singular = batch.singular
        arg_var = Macro.var(batch.key, nil)

        # Return-mode-aware spec for the generated singular entry point.
        # We don't know the user's concrete types, so the best we can do
        # is communicate the shape: scalar-or-nil-or-error for :one mode,
        # list-or-error for :list mode.
        spec_ast =
          case batch.returns do
            :one ->
              quote do
                @spec unquote(singular)(term()) :: term() | nil | {:error, Exception.t()}
              end

            :list ->
              quote do
                @spec unquote(singular)(term()) :: [term()] | {:error, Exception.t()}
              end
          end

        quote do
          unquote(spec_ast)

          def unquote(singular)(unquote(arg_var)) do
            Flurry.Runtime.call(__MODULE__, unquote(singular), unquote(arg_var))
          end
        end
      end)

    quote do
      @spec __flurry_batches__() :: [map()]
      def __flurry_batches__, do: unquote(Macro.escape(batches))

      @spec child_spec(keyword()) :: Supervisor.child_spec()
      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          type: :supervisor,
          restart: :permanent
        }
      end

      @spec start_link(keyword()) :: Supervisor.on_start()
      def start_link(opts \\ []) do
        Flurry.Supervisor.start_link(__MODULE__, __flurry_batches__(), opts)
      end

      unquote_splicing(singular_defs)
    end
  end
end
