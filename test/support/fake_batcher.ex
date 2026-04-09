defmodule Flurry.Test.FakeBatcher do
  @moduledoc """
  A `use Flurry` module used across the integration tests. Uses an in-memory
  map injected via a caller-controlled Agent instead of a real Ecto repo, so
  the tests don't need a database — they're unit tests of the batching
  pipeline itself, not of any particular data store.

  The Agent's `:records` key holds a list of `%{id: id, name: name}` maps
  shared by all tests. The Agent's `:calls` key counts how many times the
  bulk function actually ran — tests assert on this to verify batching is
  happening (N callers → 1 bulk call).

  The Agent's `:block_until` key, when set to a pid, causes the bulk function
  to block on a message from that pid before returning. Tests use this to
  deterministically hold a batch open while more requests pile up, then
  release it and check that the later requests all got bundled into the
  next batch.
  """

  use Flurry, repo: :none

  @agent __MODULE__.State

  def start_state(records) do
    # Agent.start/1 (no link) so the Agent's lifetime is bound to the
    # explicit on_exit callback, not to the test process's link graph —
    # that avoids a race between link-based teardown and `stop_state/0`.
    {:ok, _} = Agent.start(fn -> %{records: records, calls: 0, block_until: nil} end, name: @agent)
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

  def set_block(pid), do: Agent.update(@agent, &Map.put(&1, :block_until, pid))
  def clear_block, do: Agent.update(@agent, &Map.put(&1, :block_until, nil))
  def call_count, do: Agent.get(@agent, & &1.calls)
  def reset_call_count, do: Agent.update(@agent, &Map.put(&1, :calls, 0))

  @decorate batch(get(id))
  def get_many(ids) do
    state = Agent.get_and_update(@agent, fn s -> {s, %{s | calls: s.calls + 1}} end)

    if pid = state.block_until do
      send(pid, {:bulk_fn_running, self(), ids})

      receive do
        :go -> :ok
      after
        5_000 -> raise "FakeBatcher bulk fn blocked for too long"
      end
    end

    Enum.filter(state.records, &(&1.id in ids))
  end

  @decorate batch(get_by_group(group), returns: :list)
  def get_many_by_group(groups) do
    Agent.update(@agent, fn s -> %{s | calls: s.calls + 1} end)
    records = Agent.get(@agent, & &1.records)
    Enum.filter(records, &(&1.group in groups))
  end
end
