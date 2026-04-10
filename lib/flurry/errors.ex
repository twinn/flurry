defmodule Flurry.AmbiguousBatchError do
  @moduledoc """
  Raised when a `:one`-mode batch function returns multiple records with the
  same correlation key.

  The default return mode (`:one`) expects each caller's argument to map to
  exactly one record. When duplicates are detected, this exception is raised
  to indicate the function should be declared with `returns: :list`.
  """

  defexception [:message]
end

defmodule Flurry.BulkCallFailed do
  @moduledoc """
  Wraps an abnormal termination of a bulk function call.

  Used for `:exit` failures so that callers receive a uniform struct they
  can pattern-match on, rather than an opaque exit reason. Raised exceptions
  from the bulk function are not wrapped; they pass through as-is so that
  callers can match on their original type (e.g., `Postgrex.Error`).
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
