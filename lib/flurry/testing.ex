defmodule Flurry.Testing do
  @moduledoc """
  Provides test-mode helpers for Flurry.

  ## Bypass Mode

  In a test environment with `Ecto.Adapters.SQL.Sandbox`, the sandbox
  resolves connection ownership by walking `$callers`/`$ancestors` from the
  querying process. Because Flurry's consumer process has no ancestry link
  to any test, the sandbox cannot route its queries.

  Enabling bypass mode causes Flurry to skip the producer/consumer pipeline
  entirely. The bulk function runs inline in the caller's process, allowing
  the sandbox to resolve ownership normally. `async: true` tests work
  without additional configuration.

  Add the following to `test_helper.exs`:

      ExUnit.start()
      Flurry.Testing.enable_bypass_globally()

  Or equivalently, in `config/test.exs`:

      config :flurry, bypass_batching: true

  With bypass enabled, every call to a decorated function runs the bulk
  function with a singleton list in the caller's process, correlates the
  result, and returns. The batching pipeline is not exercised; unit tests
  cover only the bulk function's own logic. To test the batching pipeline
  itself, write integration tests without enabling bypass.
  """

  @doc """
  Enables bypass mode globally via `Application.put_env/3`.

  When called in `test_helper.exs`, every test in the suite skips the
  batching pipeline and runs the bulk function inline in the caller's
  process.
  """
  @spec enable_bypass_globally() :: :ok
  def enable_bypass_globally do
    Application.put_env(:flurry, :bypass_batching, true)
    :ok
  end

  @doc """
  Disables bypass mode globally.

  Allows subsequent tests to exercise the real batching pipeline.
  """
  @spec disable_bypass_globally() :: :ok
  def disable_bypass_globally do
    Application.put_env(:flurry, :bypass_batching, false)
    :ok
  end

  @doc """
  Returns `true` if Flurry is configured to bypass the batching pipeline.

  Checked by every generated entry point before dispatching to the
  producer/consumer pipeline.
  """
  @spec bypass?() :: boolean()
  def bypass? do
    Application.get_env(:flurry, :bypass_batching, false)
  end
end
