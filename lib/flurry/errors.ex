defmodule Flurry.AmbiguousBatchError do
  @moduledoc """
  Raised when a `:one`-mode batch function returns multiple records sharing
  the same correlation key. The default mode assumes each caller's argument
  maps to exactly one record; ambiguity here means the function should
  probably be declared with `returns: :list`.
  """

  defexception [:message]
end

defmodule Flurry.BulkCallFailed do
  @moduledoc """
  Wraps an abnormal termination of a bulk function call in a uniform
  exception type. Used for `:exit` (and any future non-exception failures)
  so that callers always get a struct they can pattern-match on, instead
  of an opaque exit reason or a mixed set of shapes.

  Raised exceptions from the bulk function are *not* wrapped — they pass
  through as-is, so users can continue to match on `Postgrex.Error`,
  `Ecto.Query.CastError`, etc.
  """

  @type kind :: :exit

  defexception [:kind, :reason, :message]

  @impl true
  def exception(opts) do
    kind = Keyword.fetch!(opts, :kind)
    reason = Keyword.fetch!(opts, :reason)

    %__MODULE__{
      kind: kind,
      reason: reason,
      message: "Flurry bulk call failed with #{kind}: #{inspect(reason)}"
    }
  end
end
