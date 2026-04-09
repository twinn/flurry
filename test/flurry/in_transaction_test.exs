defmodule Flurry.InTransactionTest do
  # Tests for the :in_transaction decorator modes — :safe, :warn, :bypass.
  # Uses a fake repo module that exposes a togglable checked_out? state
  # via an Agent so we can deterministically simulate being inside or
  # outside a transaction.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  defmodule FakeRepo do
    @moduledoc false
    @agent __MODULE__.State

    def start_state do
      {:ok, _} = Agent.start(fn -> %{checked_out: false} end, name: @agent)
      :ok
    end

    def stop_state do
      case Process.whereis(@agent) do
        nil ->
          :ok

        pid ->
          try do
            Agent.stop(pid)
          catch
            :exit, _ -> :ok
          end
      end
    end

    def set_checked_out(value) do
      Agent.update(@agent, &Map.put(&1, :checked_out, value))
    end

    def checked_out? do
      case Process.whereis(@agent) do
        nil -> false
        _ -> Agent.get(@agent, & &1.checked_out)
      end
    end
  end

  defmodule SafeBatcher do
    @moduledoc false
    use Flurry, repo: Flurry.InTransactionTest.FakeRepo

    @decorate batch(get(id), in_transaction: :safe)
    def get_many(ids) do
      Agent.update(__MODULE__.Sink, fn n -> n + 1 end)
      Enum.map(ids, &%{id: &1})
    end
  end

  defmodule WarnBatcher do
    @moduledoc false
    use Flurry, repo: Flurry.InTransactionTest.FakeRepo

    # :warn is the default — no explicit annotation
    @decorate batch(get(id))
    def get_many(ids) do
      Agent.update(__MODULE__.Sink, fn n -> n + 1 end)
      Enum.map(ids, &%{id: &1})
    end
  end

  defmodule BypassBatcher do
    @moduledoc false
    use Flurry, repo: Flurry.InTransactionTest.FakeRepo

    @decorate batch(get(id), in_transaction: :bypass)
    def get_many(ids) do
      Agent.update(__MODULE__.Sink, fn n -> n + 1 end)
      Enum.map(ids, &%{id: &1})
    end
  end

  setup do
    FakeRepo.start_state()
    {:ok, _} = Agent.start(fn -> 0 end, name: SafeBatcher.Sink)
    {:ok, _} = Agent.start(fn -> 0 end, name: WarnBatcher.Sink)
    {:ok, _} = Agent.start(fn -> 0 end, name: BypassBatcher.Sink)
    start_supervised!(SafeBatcher)
    start_supervised!(WarnBatcher)
    start_supervised!(BypassBatcher)

    on_exit(fn ->
      FakeRepo.stop_state()

      for name <- [SafeBatcher.Sink, WarnBatcher.Sink, BypassBatcher.Sink] do
        case Process.whereis(name) do
          nil ->
            :ok

          pid ->
            try do
              Agent.stop(pid)
            catch
              :exit, _ -> :ok
            end
        end
      end
    end)

    :ok
  end

  describe ":safe mode" do
    test "returns the correct record regardless of checked_out? state" do
      FakeRepo.set_checked_out(false)
      assert %{id: 1} = SafeBatcher.get(1)

      FakeRepo.set_checked_out(true)
      assert %{id: 2} = SafeBatcher.get(2)
    end

    test "never logs a warning even when checked_out? is true" do
      FakeRepo.set_checked_out(true)
      log = capture_log(fn -> SafeBatcher.get(1) end)
      refute log =~ "inside a transaction"
    end
  end

  describe ":warn mode (default)" do
    test "returns the correct record" do
      assert %{id: 1} = WarnBatcher.get(1)
    end

    test "logs a warning when checked_out? is true" do
      FakeRepo.set_checked_out(true)
      log = capture_log(fn -> WarnBatcher.get(1) end)
      assert log =~ "inside a transaction"
      assert log =~ "in_transaction: :safe"
      assert log =~ "in_transaction: :bypass"
    end

    test "does not log when checked_out? is false" do
      FakeRepo.set_checked_out(false)
      log = capture_log(fn -> WarnBatcher.get(1) end)
      refute log =~ "inside a transaction"
    end

    test "still returns a value even when the warning fires" do
      FakeRepo.set_checked_out(true)
      result = capture_log(fn -> send(self(), WarnBatcher.get(1)) end)
      assert result =~ "inside a transaction"
      assert_received %{id: 1}
    end
  end

  describe ":bypass mode" do
    test "runs inline when checked_out? is true" do
      FakeRepo.set_checked_out(true)
      # Record call count before and after.
      before_count = Agent.get(BypassBatcher.Sink, & &1)
      assert %{id: 1} = BypassBatcher.get(1)
      after_count = Agent.get(BypassBatcher.Sink, & &1)
      # Inline bypass calls get_many([1]) directly once.
      assert after_count - before_count == 1
    end

    test "goes through the pipeline when checked_out? is false" do
      FakeRepo.set_checked_out(false)
      assert %{id: 1} = BypassBatcher.get(1)
    end
  end

  describe "Flurry.Testing.bypass? short-circuit" do
    setup do
      Flurry.Testing.enable_bypass_globally()
      on_exit(&Flurry.Testing.disable_bypass_globally/0)
      :ok
    end

    test "bypass overrides :safe mode (runs inline)" do
      before_count = Agent.get(SafeBatcher.Sink, & &1)
      assert %{id: 1} = SafeBatcher.get(1)
      after_count = Agent.get(SafeBatcher.Sink, & &1)
      assert after_count - before_count == 1
    end

    test "bypass overrides :warn mode and suppresses the warning" do
      FakeRepo.set_checked_out(true)
      log = capture_log(fn -> assert %{id: 1} = WarnBatcher.get(1) end)
      refute log =~ "inside a transaction"
    end

    test "bypass works regardless of checked_out? state in :bypass mode" do
      FakeRepo.set_checked_out(false)
      assert %{id: 1} = BypassBatcher.get(1)
    end
  end
end
