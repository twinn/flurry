defmodule Flurry.ProducerTest do
  # Pure-function tests of the producer's emission logic. These exercise
  # `maybe_emit/2` with synthetic state + queue length values so the
  # mailbox-peek behavior is deterministic without us having to coordinate
  # real mailbox state across processes.
  use ExUnit.Case, async: true

  alias Flurry.Producer

  defp state(pending, demand, opts \\ []) do
    %{
      module: FakeMod,
      batch: %{singular: :get, key: :id, bulk: :get_many, returns: :one, arity: 1},
      batch_size: Keyword.get(opts, :batch_size, 100),
      pending: pending,
      demand: demand,
      priority: Keyword.get(opts, :priority, [])
    }
  end

  defp entry(id), do: {id, {self(), make_ref()}}

  describe "maybe_emit/2 — empty cases" do
    test "no pending → no events" do
      assert {[], s} = Producer.maybe_emit(state([], 1), 0)
      assert s.pending == []
    end

    test "no demand → no events even when pending + mailbox empty" do
      assert {[], s} = Producer.maybe_emit(state([entry(1)], 0), 0)
      assert length(s.pending) == 1
    end
  end

  describe "maybe_emit/2 — mailbox-empty branch" do
    test "pending + demand + empty mailbox → flush everything as one batch" do
      pending = Enum.map([1, 2, 3], &entry/1)
      assert {[event], s} = Producer.maybe_emit(state(pending, 1), 0)
      assert {:flurry_batch, FakeMod, _batch, entries} = event
      assert length(entries) == 3
      assert s.pending == []
      assert s.demand == 0
    end

    test "single pending with empty mailbox → flush as singleton" do
      assert {[event], _s} = Producer.maybe_emit(state([entry(42)], 1), 0)
      assert {:flurry_batch, _, _, [{42, _}]} = event
    end
  end

  describe "maybe_emit/2 — non-empty mailbox" do
    test "pending < batch_size with non-empty mailbox → hold" do
      pending = Enum.map([1, 2], &entry/1)
      assert {[], s} = Producer.maybe_emit(state(pending, 1, batch_size: 100), 5)
      assert length(s.pending) == 2
    end

    test "pending >= batch_size with non-empty mailbox → flush anyway" do
      pending = Enum.map(1..10, &entry/1)
      assert {[event], s} = Producer.maybe_emit(state(pending, 1, batch_size: 10), 5)
      assert {:flurry_batch, _, _, entries} = event
      assert length(entries) == 10
      assert s.pending == []
    end

    test "pending > batch_size with non-empty mailbox → flush batch_size, keep rest" do
      pending = Enum.map(1..15, &entry/1)
      assert {[event], s} = Producer.maybe_emit(state(pending, 1, batch_size: 10), 5)
      assert {:flurry_batch, _, _, entries} = event
      assert length(entries) == 10
      assert length(s.pending) == 5
    end
  end

  describe "maybe_emit/2 — demand accounting" do
    test "flushing one batch decrements demand by 1" do
      assert {[_], s} = Producer.maybe_emit(state([entry(1)], 3), 0)
      assert s.demand == 2
    end
  end

  describe "priority queue" do
    # A priority queue holds pre-formed batches that were bisected by the
    # consumer and need to be re-tried AS-IS — not merged with each other
    # and not merged with incoming pending requests. Emission drains
    # priority in FIFO order, one batch per event, before it even looks at
    # pending.

    test "with priority non-empty and demand > 0, flushes first priority batch as one event" do
      priority_batch = Enum.map([1, 2, 3], &entry/1)
      new_pending = [entry(99)]

      assert {[event], s} =
               Producer.maybe_emit(
                 state(new_pending, 1, priority: [priority_batch]),
                 0
               )

      assert {:flurry_batch, _, _, entries} = event
      # The event contains exactly the priority batch, not the pending.
      assert length(entries) == 3
      assert Enum.map(entries, fn {arg, _} -> arg end) == [1, 2, 3]

      # Priority drained, pending untouched.
      assert s.priority == []
      assert length(s.pending) == 1
      assert s.demand == 0
    end

    test "multiple priority batches flush FIFO, one per call" do
      b1 = Enum.map([1, 2], &entry/1)
      b2 = Enum.map([3, 4], &entry/1)
      b3 = Enum.map([5, 6], &entry/1)

      s0 = state([], 1, priority: [b1, b2, b3])

      assert {[{:flurry_batch, _, _, e1}], s1} = Producer.maybe_emit(s0, 0)
      assert Enum.map(e1, fn {a, _} -> a end) == [1, 2]
      assert length(s1.priority) == 2

      s1 = %{s1 | demand: 1}
      assert {[{:flurry_batch, _, _, e2}], s2} = Producer.maybe_emit(s1, 0)
      assert Enum.map(e2, fn {a, _} -> a end) == [3, 4]
      assert length(s2.priority) == 1

      s2 = %{s2 | demand: 1}
      assert {[{:flurry_batch, _, _, e3}], s3} = Producer.maybe_emit(s2, 0)
      assert Enum.map(e3, fn {a, _} -> a end) == [5, 6]
      assert s3.priority == []
    end

    test "with priority non-empty and demand == 0, holds" do
      priority_batch = [entry(1)]
      assert {[], s} = Producer.maybe_emit(state([], 0, priority: [priority_batch]), 0)
      assert length(s.priority) == 1
    end

    test "after priority drains, emission falls back to pending logic" do
      s0 = state([entry(1)], 1)
      assert {[{:flurry_batch, _, _, entries}], s1} = Producer.maybe_emit(s0, 0)
      assert Enum.map(entries, fn {a, _} -> a end) == [1]
      assert s1.pending == []
    end

    test "priority flush does NOT mix with pending even if pending is non-empty" do
      priority_batch = [entry(1)]
      # Pending has a bunch of stuff that would otherwise get flushed.
      pending = Enum.map([10, 11, 12], &entry/1)

      assert {[{:flurry_batch, _, _, entries}], s} =
               Producer.maybe_emit(
                 state(pending, 1, priority: [priority_batch]),
                 0
               )

      # Exactly the priority batch, nothing more.
      assert Enum.map(entries, fn {arg, _} -> arg end) == [1]
      # Pending still has all 3 waiting.
      assert length(s.pending) == 3
    end
  end

  describe "multi-cycle drain" do
    # Simulates what happens when the producer has many more items than can
    # fit in one batch_size cap. Each maybe_emit call should flush exactly
    # one batch of at most batch_size, decrement demand, and leave the rest
    # pending. Subsequent calls (triggered by handle_demand from the
    # consumer) drain progressively until pending is empty.
    test "emits one capped batch per maybe_emit call; remainder flushes next cycle" do
      pending = Enum.map(1..25, &entry/1)
      s0 = state(pending, 1, batch_size: 10)

      # Cycle 1: emit first 10, keep 15, demand exhausted.
      {[{:flurry_batch, _, _, batch1}], s1} = Producer.maybe_emit(s0, 0)
      assert length(batch1) == 10
      assert length(s1.pending) == 15
      assert s1.demand == 0

      # Simulate consumer asking for more demand.
      s1 = %{s1 | demand: 1}

      # Cycle 2: emit next 10, keep 5.
      {[{:flurry_batch, _, _, batch2}], s2} = Producer.maybe_emit(s1, 0)
      assert length(batch2) == 10
      assert length(s2.pending) == 5
      assert s2.demand == 0

      # Cycle 3: emit remaining 5.
      s2 = %{s2 | demand: 1}
      {[{:flurry_batch, _, _, batch3}], s3} = Producer.maybe_emit(s2, 0)
      assert length(batch3) == 5
      assert s3.pending == []
      assert s3.demand == 0
    end
  end
end
