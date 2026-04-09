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

    overridable = Keyword.get(opts, :overridable, [])
    validate_overridable_list!(overridable)

    # Emit a delegate + defoverridable for each overridable entry. The
    # delegate calls `_flurry_<name>/N`, which __before_compile__ will
    # generate with the full batched body. Forward reference is safe —
    # Elixir resolves same-module function calls at call time.
    overridable_defs =
      for {name, arity} <- overridable do
        vars = for i <- 1..arity, do: Macro.var(:"arg#{i}", nil)
        helper_name = :"_flurry_#{name}"

        quote do
          def unquote(name)(unquote_splicing(vars)) do
            unquote(helper_name)(unquote_splicing(vars))
          end

          defoverridable [{unquote(name), unquote(arity)}]
        end
      end

    quote do
      use Flurry.Decorators

      Module.register_attribute(__MODULE__, :flurry_batches, accumulate: true)
      Module.put_attribute(__MODULE__, :flurry_repo, unquote(repo))
      Module.put_attribute(__MODULE__, :flurry_overridable, unquote(overridable))
      @before_compile Flurry

      unquote_splicing(overridable_defs)
    end
  end

  defp validate_overridable_list!(overridable) when is_list(overridable) do
    if !Keyword.keyword?(overridable) do
      raise ArgumentError,
            "Flurry: `:overridable` must be a keyword list of name: arity pairs, " <>
              "e.g. `overridable: [get: 1, get_by_email: 1]`. Got: #{inspect(overridable)}"
    end

    for {name, arity} <- overridable do
      if !(is_atom(name) and is_integer(arity) and arity > 0) do
        raise ArgumentError,
              "Flurry: invalid `:overridable` entry #{inspect({name, arity})} — " <>
                "each entry must be `atom: positive_integer`."
      end
    end

    :ok
  end

  defp validate_overridable_list!(other) do
    raise ArgumentError,
          "Flurry: `:overridable` must be a keyword list of name: arity pairs, " <>
            "got: #{inspect(other)}"
  end

  @doc false
  defmacro __before_compile__(env) do
    repo = Module.get_attribute(env.module, :flurry_repo)
    overridable = Module.get_attribute(env.module, :flurry_overridable) || []
    raw_batches = env.module |> Module.get_attribute(:flurry_batches) |> Enum.reverse()
    batches = Enum.map(raw_batches, &resolve_in_transaction(&1, repo, env.module))

    :ok = validate_overridable_vs_batches!(overridable, batches, env.module)

    # Persist the resolved batches back so `__flurry_batches__/0` reflects
    # the post-resolution view.
    Module.delete_attribute(env.module, :flurry_batches)
    Enum.each(batches, &Module.put_attribute(env.module, :flurry_batches, &1))
    batches = Enum.reverse(batches)

    return_types = %{
      one: quote(do: term() | nil | {:error, Exception.t()}),
      list: quote(do: [term()] | {:error, Exception.t()})
    }

    singular_defs =
      for batch <- batches do
        build_batch_entry_defs(batch, repo, return_types, overridable)
      end

    # `:batch_by` is compile-time-only AST — strip it from the runtime
    # batch maps so Macro.escape doesn't try to serialize it.
    runtime_batches = Enum.map(batches, &Map.delete(&1, :batch_by))

    quote do
      @spec __flurry_batches__() :: [map()]
      def __flurry_batches__, do: unquote(Macro.escape(runtime_batches))

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

  # Builds the generated code for one batch. Always emits a
  # `_flurry_<singular>/N` helper containing the full batched implementation.
  # The public `<singular>/N` is a thin delegate — when :overridable lists
  # this name, __using__ already injected the delegate + defoverridable, so
  # we skip emitting it here to avoid a redefinition.
  defp build_batch_entry_defs(batch, repo, return_types, overridable) do
    batched_var = Macro.var(batch.key, nil)
    group_vars = Enum.map(batch.group_args, &Macro.var(&1, nil))
    all_vars = [batched_var | group_vars]
    arity = length(all_vars)

    spec_arg_types = List.duplicate(quote(do: term()), arity)
    return_type = return_types[batch.returns]

    raw_group_tuple_ast = {:{}, [], group_vars}
    normalizer_ast = batch.batch_by || quote(do: & &1)
    group_tuple_ast = quote(do: unquote(normalizer_ast).(unquote(raw_group_tuple_ast)))

    body_ast = build_entry_body(batch, repo, batched_var, all_vars, group_tuple_ast)

    helper_name = :"_flurry_#{batch.singular}"
    is_overridable = Keyword.get(overridable, batch.singular) == arity

    helper_def =
      quote do
        @doc false
        @spec unquote(helper_name)(unquote_splicing(spec_arg_types)) :: unquote(return_type)
        def unquote(helper_name)(unquote_splicing(all_vars)) do
          unquote(body_ast)
        end
      end

    # When the user opted into `overridable: [name: arity]`, the public
    # `name/arity` delegate and `defoverridable` were emitted in __using__.
    # We skip re-emitting the delegate here to avoid redefinition.
    public_def =
      if is_overridable do
        quote(do: nil)
      else
        quote do
          @spec unquote(batch.singular)(unquote_splicing(spec_arg_types)) ::
                  unquote(return_type)
          def unquote(batch.singular)(unquote_splicing(all_vars)) do
            unquote(helper_name)(unquote_splicing(all_vars))
          end
        end
      end

    quote do
      unquote(helper_def)
      unquote(public_def)
    end
  end

  # Raises CompileError if any :overridable entry has no matching
  # @decorate batch, or if arities don't line up.
  defp validate_overridable_vs_batches!(overridable, batches, module) do
    for {name, declared_arity} <- overridable do
      batch =
        Enum.find(batches, &(&1.singular == name)) ||
          raise CompileError,
            description:
              "Flurry: #{inspect(module)} declared `overridable: [#{name}: #{declared_arity}]` " <>
                "but no `@decorate batch(#{name}(...))` decoration was found. " <>
                "Either add a matching decorator on a bulk function in this module, or " <>
                "remove #{inspect(name)} from the :overridable list."

      actual_arity = 1 + length(batch.group_args)

      if actual_arity != declared_arity do
        raise CompileError,
          description:
            "Flurry: #{inspect(module)} declared `overridable: [#{name}: #{declared_arity}]` " <>
              "but the `@decorate batch(#{name}(...))` on #{batch.bulk}/#{batch.arity} " <>
              "has arity #{actual_arity} (1 batched arg + #{length(batch.group_args)} group args). " <>
              "Update the :overridable arity to match."
      end
    end

    :ok
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
