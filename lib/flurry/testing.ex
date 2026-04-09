defmodule Flurry.Testing do
  @moduledoc """
  Test-mode helpers for Flurry.

  ## Bypass mode

  In a test environment with `Ecto.Adapters.SQL.Sandbox`, the sandbox
  walks `$callers` / `$ancestors` from the querying process to find the
  test that owns the checked-out connection. Flurry's default pipeline
  runs the bulk function in a background consumer process that has **no
  ancestry link** to any test, so the sandbox cannot route its queries.

  Enabling *bypass mode* makes Flurry skip the producer/consumer pipeline
  entirely: the bulk function runs inline in the caller's process, the
  sandbox walks ancestry normally, and `async: true` tests work.

  Add this to your `test_helper.exs`:

      ExUnit.start()
      Flurry.Testing.enable_bypass_globally()

  Or equivalently:

      # config/test.exs
      config :flurry, bypass_batching: true

  With bypass enabled, every call to a decorated function runs the bulk
  function with a singleton list in the caller's process, correlates
  the result, and returns. The batching pipeline is not exercised —
  the tradeoff is that unit tests don't cover producer/consumer behavior,
  only the bulk function's own logic. If you want to test the batching
  pipeline itself, write integration tests that don't enable bypass.
  """

  @doc """
  Enables bypass mode globally via `Application.put_env/3`. Call this in
  your `test_helper.exs` so that every test in your suite skips the
  batching pipeline and runs the bulk function inline.
  """
  @spec enable_bypass_globally() :: :ok
  def enable_bypass_globally do
    Application.put_env(:flurry, :bypass_batching, true)
    :ok
  end

  @doc """
  Disables bypass mode globally. Useful if you want specific tests to
  exercise the real batching pipeline.
  """
  @spec disable_bypass_globally() :: :ok
  def disable_bypass_globally do
    Application.put_env(:flurry, :bypass_batching, false)
    :ok
  end

  @doc """
  Returns `true` if Flurry is configured to bypass the batching pipeline
  and run bulk functions inline in the caller's process. Checked by every
  generated entry point before dispatching.
  """
  @spec bypass?() :: boolean()
  def bypass? do
    Application.get_env(:flurry, :bypass_batching, false)
  end
end
