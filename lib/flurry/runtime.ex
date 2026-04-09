defmodule Flurry.Runtime do
  @moduledoc false
  # Runtime helpers used by the generated singular entry points and by the
  # per-module supervisor. These are internal; the public surface is the
  # `use Flurry` macro and the functions it generates on the user's module.

  @doc """
  Called by the generated singular entry-point functions. Enqueues a single
  request into the producer for `{module, singular}` and blocks on the reply.

  `group_key` is a tuple of the non-batched arguments (empty `{}` for
  single-arg decorations). Callers sharing the same group key get coalesced
  into the same batch; distinct groups run as independent batches.
  """
  @spec call(module(), atom(), term(), tuple()) :: term()
  def call(module, singular, arg, group_key) when is_tuple(group_key) do
    producer = producer_name(module, singular)
    GenServer.call(producer, {:enqueue, group_key, arg}, 5_000)
  end

  @doc "Canonical producer name for a given user module + singular function."
  @spec producer_name(module(), atom()) :: atom()
  def producer_name(module, singular) do
    Module.concat([module, camelize(singular), "Producer"])
  end

  @doc "Canonical consumer name for a given user module + singular function."
  @spec consumer_name(module(), atom()) :: atom()
  def consumer_name(module, singular) do
    Module.concat([module, camelize(singular), "Consumer"])
  end

  defp camelize(atom) when is_atom(atom), do: atom |> Atom.to_string() |> Macro.camelize()
end
