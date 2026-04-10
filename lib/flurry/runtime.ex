defmodule Flurry.Runtime do
  @moduledoc false
  # Runtime helpers used by the generated singular entry points and by the
  # per-module supervisor. These are internal; the public surface is the
  # `use Flurry` macro and the functions it generates on the user's module.

  @doc """
  Enqueues a single request into the producer for `{module, singular}` and
  blocks until the reply is received.

  `group_key` is a tuple of the non-batched arguments (empty `{}` for
  single-arg decorations). Callers sharing the same group key are coalesced
  into the same batch; distinct groups run as independent batches.

  `timeout` is the `GenServer.call/3` timeout controlling how long the
  caller blocks waiting for the batched result.
  """
  @spec call(module(), atom(), term(), tuple(), timeout()) :: term()
  def call(module, singular, arg, group_key, timeout) when is_tuple(group_key) do
    producer = producer_name(module, singular)
    GenServer.call(producer, {:enqueue, group_key, arg}, timeout)
  end

  @doc "Returns the canonical producer name for the given module and singular function."
  @spec producer_name(module(), atom()) :: atom()
  def producer_name(module, singular) do
    Module.concat([module, camelize(singular), "Producer"])
  end

  @doc "Returns the canonical consumer name for the given module and singular function."
  @spec consumer_name(module(), atom()) :: atom()
  def consumer_name(module, singular) do
    Module.concat([module, camelize(singular), "Consumer"])
  end

  @doc """
  Emits a warning when the caller is inside a transaction.

  Invoked by `:warn`-mode generated entry points. When `repo.checked_out?/0`
  returns `true`, logs a warning describing the implications and the
  available `:in_transaction` options.
  """
  @spec maybe_warn_in_transaction(module(), atom(), module()) :: :ok
  def maybe_warn_in_transaction(module, fn_name, repo) do
    if repo.checked_out?() do
      require Logger

      Logger.warning("""
      Flurry: #{inspect(module)}.#{fn_name} called inside a transaction. The \
      batched call will run on a separate DB connection, which means:

        * any WRITES performed by the bulk function will NOT be rolled back \
      if the caller's transaction rolls back — committed state can survive \
      a "failed" transaction. This is a data integrity issue for any \
      mutation (insert/update/delete) batcher.

        * any READS performed by the bulk function will NOT see uncommitted \
      writes from the caller's transaction (no read-your-writes consistency).

      For mutations, add `in_transaction: :bypass` to run the bulk function \
      inline in the caller's process so it participates in the transaction:

          @decorate batch(#{fn_name}(arg), in_transaction: :bypass)

      For reads that don't need read-your-writes consistency, add \
      `in_transaction: :safe` to silence this warning:

          @decorate batch(#{fn_name}(arg), in_transaction: :safe)
      """)
    end

    :ok
  end

  defp camelize(atom) when is_atom(atom), do: atom |> Atom.to_string() |> Macro.camelize()
end
