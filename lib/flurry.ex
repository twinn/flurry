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
  defmacro __using__(opts) do
    repo =
      case Keyword.fetch(opts, :repo) do
        {:ok, value} ->
          value

        :error ->
          raise ArgumentError, """
          Flurry: `use Flurry` requires a `:repo` option.

              use Flurry, repo: MyApp.Repo   # uses the repo's checked_out?/0
                                             # to detect transactions
              use Flurry, repo: :none        # no transaction semantics;
                                             # always batches

          The `:repo` option exists so that batched function calls made
          from inside a `Repo.transaction/2` (or any context where a
          connection has been checked out) can be detected and handled
          per the `:in_transaction` decorator option. Pass `:none` only
          if this module batches operations that have no database
          context (e.g. external API calls).
          """
      end

    quote bind_quoted: [repo: repo] do
      use Flurry.Decorators

      Module.register_attribute(__MODULE__, :flurry_batches, accumulate: true)
      Module.put_attribute(__MODULE__, :flurry_repo, repo)
      @before_compile Flurry
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    repo = Module.get_attribute(env.module, :flurry_repo)
    raw_batches = env.module |> Module.get_attribute(:flurry_batches) |> Enum.reverse()
    batches = Enum.map(raw_batches, &resolve_in_transaction(&1, repo, env.module))

    # Persist the resolved batches back so `__flurry_batches__/0` reflects
    # the post-resolution view.
    Module.delete_attribute(env.module, :flurry_batches)

    Enum.each(batches, fn batch ->
      Module.put_attribute(env.module, :flurry_batches, batch)
    end)

    batches = Enum.reverse(batches)

    singular_defs =
      Enum.map(batches, fn batch ->
        singular = batch.singular
        batched_var = Macro.var(batch.key, nil)
        group_vars = Enum.map(batch.group_args, &Macro.var(&1, nil))
        all_vars = [batched_var | group_vars]

        # Return-mode-aware spec for the generated singular entry point.
        spec_arg_types = List.duplicate(quote(do: term()), length(all_vars))

        return_type =
          case batch.returns do
            :one -> quote(do: term() | nil | {:error, Exception.t()})
            :list -> quote(do: [term()] | {:error, Exception.t()})
          end

        spec_ast =
          quote do
            @spec unquote(singular)(unquote_splicing(spec_arg_types)) :: unquote(return_type)
          end

        # The group key tuple AST: `{user_id, active?}` for group_vars
        # `[user_id, active?]`; `{}` for single-arg decorations.
        group_tuple_ast =
          case group_vars do
            [] -> quote(do: {})
            _ -> {:{}, [], group_vars}
          end

        body_ast =
          build_entry_body(
            batch,
            repo,
            batched_var,
            all_vars,
            group_tuple_ast
          )

        quote do
          unquote(spec_ast)

          def unquote(singular)(unquote_splicing(all_vars)) do
            unquote(body_ast)
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

  # Fills in the default :in_transaction value based on the module's :repo
  # setting, and validates illegal combinations.
  defp resolve_in_transaction(batch, repo, module) do
    default =
      case repo do
        :none -> :safe
        _ -> :warn
      end

    resolved = batch.in_transaction || default

    if repo == :none and resolved in [:warn, :bypass] do
      raise ArgumentError, """
      Flurry: #{inspect(module)}.#{batch.bulk}/#{batch.arity} has \
      `in_transaction: #{inspect(resolved)}`, but this module is configured \
      with `repo: :none`. `:in_transaction` modes other than `:safe` \
      require a real `:repo` module so Flurry can call `checked_out?/0` \
      on it.

      Either:
        1. Set a real repo on `use Flurry`, or
        2. Use `in_transaction: :safe` on this decorator (or omit it — \
      `:safe` is the default when `repo: :none`).
      """
    end

    %{batch | in_transaction: resolved}
  end

  # Emits the entry-point body for a decorated function, varying by
  # in_transaction mode. Every mode short-circuits to inline execution
  # when `Flurry.Testing.bypass?()` is true.
  defp build_entry_body(batch, repo, batched_var, all_vars, group_tuple_ast) do
    singular = batch.singular
    timeout = batch.timeout

    runtime_call_ast =
      quote do
        Flurry.Runtime.call(
          __MODULE__,
          unquote(singular),
          unquote(batched_var),
          unquote(group_tuple_ast),
          unquote(timeout)
        )
      end

    inline_ast = build_inline_body(batch, batched_var, all_vars)

    case batch.in_transaction do
      :safe ->
        quote do
          if Flurry.Testing.bypass?() do
            unquote(inline_ast)
          else
            unquote(runtime_call_ast)
          end
        end

      :warn ->
        quote do
          if Flurry.Testing.bypass?() do
            unquote(inline_ast)
          else
            Flurry.Runtime.maybe_warn_in_transaction(
              __MODULE__,
              unquote(singular),
              unquote(repo)
            )

            unquote(runtime_call_ast)
          end
        end

      :bypass ->
        quote do
          if Flurry.Testing.bypass?() or unquote(repo).checked_out?() do
            unquote(inline_ast)
          else
            unquote(runtime_call_ast)
          end
        end
    end
  end

  # Inline execution: run the bulk function in the caller's process with
  # a singleton list and correlate the result. Used by test bypass and
  # by `:bypass` mode when inside a transaction.
  defp build_inline_body(batch, batched_var, all_vars) do
    bulk = batch.bulk
    correlate = batch.correlate
    returns = batch.returns
    group_vars = tl(all_vars)

    bulk_call_args = [quote(do: [unquote(batched_var)]) | group_vars]

    quote do
      result = apply(__MODULE__, unquote(bulk), unquote(bulk_call_args))
      correlated = Flurry.Consumer.correlate(result, unquote(correlate), unquote(returns))
      Flurry.Consumer.lookup(correlated, unquote(batched_var), unquote(returns))
    end
  end
end
