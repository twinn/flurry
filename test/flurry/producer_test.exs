defmodule Flurry.ProducerTest do
  # Pure-function tests of the producer's emission logic. These exercise
  # `maybe_emit/2` with synthetic state + queue length values so the
  # mailbox-peek behavior is deterministic without us having to coordinate
  # real mailbox state across processes.
  use ExUnit.Case, async: true

  alias Flurry.Producer

  # Default group key for tests that only care about single-group behavior.
  @g {}

  defp state(pending_map, demand, opts \\ []) do
    group_order = Keyword.get(opts, :group_order, Map.keys(pending_map))

    %{
      module: FakeMod,
      batch: %{
        singular: :get,
        key: :id,
        bulk: :get_many,
        returns: :one,
        arity: 1,
        additive_positions: Keyword.get(opts, :additive_positions, [])
      },
      batch_size: Keyword.get(opts, :batch_size, 100),
      pending: pending_map,
      group_order: group_order,
      demand: demand,
      priority: Keyword.get(opts, :priority, []),
      force_flush: Keyword.get(opts, :force_flush, false)
    }
  end

  defp single_group(entries, demand, opts \\ []) do
    state(%{@g => entries}, demand, opts)
  end

  # Pending entries inside the producer carry a 3-tuple
  # `{arg, additive_values, from}`. For non-additive batches
  # additive_values is always `[]`.
  defp entry(id), do: {id, [], {self(), make_ref()}}
  defp entry(id, additive), do: {id, additive, {self(), make_ref()}}

  # Priority items and emitted events carry the stripped 2-tuple
  # `{arg, from}` — this is what downstream consumers see.
  defp stripped_entry(id), do: {id, {self(), make_ref()}}

  describe "maybe_emit/2 — empty cases" do
    test "no pending → no events" do
      assert {[], s} = Producer.maybe_emit(state(%{}, 1), 0)
      assert s.pending == %{}
      assert s.group_order == []
    end

    test "no demand → no events even when pending + mailbox empty" do
      assert {[], s} = Producer.maybe_emit(single_group([entry(1)], 0), 0)
      assert length(s.pending[@g]) == 1
    end
  end

  describe "maybe_emit/2 — mailbox-empty branch" do
    test "pending + demand + empty mailbox → flush everything as one batch" do
      pending = Enum.map([1, 2, 3], &entry/1)
      assert {[event], s} = Producer.maybe_emit(single_group(pending, 1), 0)
      assert {:flurry_batch, FakeMod, _batch, @g, entries} = event
      assert length(entries) == 3
      # Empty group dropped from pending map.
      assert s.pending == %{}
      assert s.group_order == []
      assert s.demand == 0
    end

    test "single pending with empty mailbox → flush as singleton" do
      assert {[event], _s} = Producer.maybe_emit(single_group([entry(42)], 1), 0)
      assert {:flurry_batch, _, _, @g, [{42, _}]} = event
    end
  end

  describe "maybe_emit/2 — non-empty mailbox" do
    test "pending < batch_size with non-empty mailbox → hold" do
      pending = Enum.map([1, 2], &entry/1)
      assert {[], s} = Producer.maybe_emit(single_group(pending, 1, batch_size: 100), 5)
      assert length(s.pending[@g]) == 2
    end

    test "pending >= batch_size with non-empty mailbox → flush anyway" do
      pending = Enum.map(1..10, &entry/1)
      assert {[event], s} = Producer.maybe_emit(single_group(pending, 1, batch_size: 10), 5)
      assert {:flurry_batch, _, _, @g, entries} = event
      assert length(entries) == 10
      assert s.pending == %{}
    end

    test "pending > batch_size with non-empty mailbox → flush batch_size, keep rest" do
      pending = Enum.map(1..15, &entry/1)
      assert {[event], s} = Producer.maybe_emit(single_group(pending, 1, batch_size: 10), 5)
      assert {:flurry_batch, _, _, @g, entries} = event
      assert length(entries) == 10
      assert length(s.pending[@g]) == 5
      # Group still tracked because not empty.
      assert s.group_order == [@g]
    end
  end

  describe "maybe_emit/2 — demand accounting" do
    test "flushing one batch decrements demand by 1" do
      assert {[_], s} = Producer.maybe_emit(single_group([entry(1)], 3), 0)
      assert s.demand == 2
    end
  end

  describe "priority queue" do
    # A priority queue holds pre-formed batches that were bisected by the
    # consumer and need to be re-tried AS-IS — not merged with each other
    # and not merged with incoming pending requests. Priority items are
    # tagged with their group key so that requeues route back to the
    # correct group.
    test "with priority non-empty and demand > 0, flushes first priority batch as one event" do
      priority_batch = Enum.map([1, 2, 3], &stripped_entry/1)
      new_pending = [entry(99)]

      assert {[event], s} =
               Producer.maybe_emit(
                 single_group(new_pending, 1, priority: [{@g, priority_batch}]),
                 0
               )

      assert {:flurry_batch, _, _, @g, entries} = event
      # The event contains exactly the priority batch, not the pending.
      assert length(entries) == 3
      assert Enum.map(entries, fn {arg, _} -> arg end) == [1, 2, 3]

      # Priority drained, pending untouched.
      assert s.priority == []
      assert length(s.pending[@g]) == 1
      assert s.demand == 0
    end

    test "multiple priority batches flush FIFO, one per call" do
      b1 = Enum.map([1, 2], &stripped_entry/1)
      b2 = Enum.map([3, 4], &stripped_entry/1)
      b3 = Enum.map([5, 6], &stripped_entry/1)

      s0 = state(%{}, 1, priority: [{@g, b1}, {@g, b2}, {@g, b3}])

      assert {[{:flurry_batch, _, _, @g, e1}], s1} = Producer.maybe_emit(s0, 0)
      assert Enum.map(e1, fn {a, _} -> a end) == [1, 2]
      assert length(s1.priority) == 2

      s1 = %{s1 | demand: 1}
      assert {[{:flurry_batch, _, _, @g, e2}], s2} = Producer.maybe_emit(s1, 0)
      assert Enum.map(e2, fn {a, _} -> a end) == [3, 4]
      assert length(s2.priority) == 1

      s2 = %{s2 | demand: 1}
      assert {[{:flurry_batch, _, _, @g, e3}], s3} = Producer.maybe_emit(s2, 0)
      assert Enum.map(e3, fn {a, _} -> a end) == [5, 6]
      assert s3.priority == []
    end

    test "with priority non-empty and demand == 0, holds" do
      priority_batch = [stripped_entry(1)]
      assert {[], s} = Producer.maybe_emit(state(%{}, 0, priority: [{@g, priority_batch}]), 0)
      assert length(s.priority) == 1
    end

    test "after priority drains, emission falls back to pending logic" do
      s0 = single_group([entry(1)], 1)
      assert {[{:flurry_batch, _, _, @g, entries}], _s1} = Producer.maybe_emit(s0, 0)
      assert Enum.map(entries, fn {a, _} -> a end) == [1]
    end

    test "priority flush does NOT mix with pending even if pending is non-empty" do
      priority_batch = [stripped_entry(1)]
      pending = Enum.map([10, 11, 12], &entry/1)

      assert {[{:flurry_batch, _, _, @g, entries}], s} =
               Producer.maybe_emit(
                 single_group(pending, 1, priority: [{@g, priority_batch}]),
                 0
               )

      assert Enum.map(entries, fn {arg, _} -> arg end) == [1]
      assert length(s.pending[@g]) == 3
    end
  end

  describe "multi-cycle drain" do
    test "emits one capped batch per maybe_emit call; remainder flushes next cycle" do
      pending = Enum.map(1..25, &entry/1)
      s0 = single_group(pending, 1, batch_size: 10)

      {[{:flurry_batch, _, _, @g, batch1}], s1} = Producer.maybe_emit(s0, 0)
      assert length(batch1) == 10
      assert length(s1.pending[@g]) == 15
      assert s1.demand == 0

      s1 = %{s1 | demand: 1}
      {[{:flurry_batch, _, _, @g, batch2}], s2} = Producer.maybe_emit(s1, 0)
      assert length(batch2) == 10
      assert length(s2.pending[@g]) == 5

      s2 = %{s2 | demand: 1}
      {[{:flurry_batch, _, _, @g, batch3}], s3} = Producer.maybe_emit(s2, 0)
      assert length(batch3) == 5
      assert s3.pending == %{}
      assert s3.group_order == []
    end
  end

  describe "multi-group" do
    # Multi-group tests: the producer holds per-group pending lists and
    # per-group priority items. Emission picks which group to flush based
    # on (a) batch_size saturation of any specific group, or (b) LRU
    # ordering via group_order when mailbox is empty.

    @ga {1, true}
    @gb {2, true}
    @gc {1, false}

    test "distinct groups get flushed as separate events, not merged" do
      entry_a = entry(1)
      entry_b = entry(2)

      s0 =
        state(
          %{@ga => [entry_a], @gb => [entry_b]},
          1,
          group_order: [@ga, @gb]
        )

      # First flush: oldest group in group_order. The event's entries
      # are stripped to the 2-tuple `{arg, from}` form, so we match
      # against that rather than the internal 3-tuple entry.
      {arg_a, _additive_a, from_a} = entry_a
      stripped_a = {arg_a, from_a}

      assert {[{:flurry_batch, _, _, @ga, [^stripped_a]}], s1} = Producer.maybe_emit(s0, 0)
      # Group A's entry was flushed and the group is empty — drop it from pending AND group_order.
      refute Map.has_key?(s1.pending, @ga)
      refute @ga in s1.group_order
      # Group B still waiting.
      assert s1.pending[@gb] == [entry_b]
      assert @gb in s1.group_order
    end

    test "LRU: when mailbox empty, flushes the front of group_order first" do
      # group_order [ga, gb, gc] means ga is least-recently-used.
      pending = %{
        @ga => [entry(1)],
        @gb => [entry(2)],
        @gc => [entry(3)]
      }

      s0 = state(pending, 1, group_order: [@ga, @gb, @gc])

      assert {[{:flurry_batch, _, _, @ga, _}], _} = Producer.maybe_emit(s0, 0)
    end

    test "after flushing a group with remaining entries, it rotates to the back of group_order" do
      pending = %{
        @ga => Enum.map(1..5, &entry/1),
        @gb => [entry(10)]
      }

      s0 = state(pending, 1, group_order: [@ga, @gb], batch_size: 3)

      # Group A has 5 pending > batch_size=3, so it flushes 3 and keeps 2.
      # It should rotate to the back of group_order.
      assert {[{:flurry_batch, _, _, @ga, batch}], s1} = Producer.maybe_emit(s0, 5)
      assert length(batch) == 3
      assert length(s1.pending[@ga]) == 2
      # Rotated: @gb now comes before @ga.
      assert s1.group_order == [@gb, @ga]
    end

    test "a group that saturates batch_size is flushed even when mailbox is non-empty" do
      # ga has 10, which >= batch_size 10 → flush regardless of mailbox.
      # gb has only 2, should not flush yet (under batch_size, non-empty mailbox).
      pending = %{
        @ga => Enum.map(1..10, &entry/1),
        @gb => [entry(100), entry(101)]
      }

      s0 = state(pending, 1, group_order: [@gb, @ga], batch_size: 10)

      assert {[{:flurry_batch, _, _, @ga, _}], s1} = Producer.maybe_emit(s0, 5)
      # gb is unchanged.
      assert length(s1.pending[@gb]) == 2
      # ga emptied out → dropped.
      refute Map.has_key?(s1.pending, @ga)
    end

    test "empty groups are dropped from pending AND group_order on flush" do
      s0 = state(%{@ga => [entry(1)]}, 1, group_order: [@ga])
      assert {[_], s1} = Producer.maybe_emit(s0, 0)
      assert s1.pending == %{}
      assert s1.group_order == []
    end

    test "priority items are routed to their tagged group and don't cross-pollinate" do
      # Pending has work in gb, priority has a batch for ga.
      pending = %{@gb => Enum.map(1..3, &entry/1)}
      # Priority batches use the 2-tuple {arg, from} form that appears
      # in events, not the 3-tuple pending-entry form.
      priority_batch_a = Enum.map(100..102, &stripped_entry/1)
      s0 = state(pending, 1, group_order: [@gb], priority: [{@ga, priority_batch_a}])

      # Priority wins: emit ga's priority batch. gb pending untouched.
      assert {[{:flurry_batch, _, _, @ga, entries}], s1} = Producer.maybe_emit(s0, 0)
      assert Enum.map(entries, fn {a, _} -> a end) == [100, 101, 102]
      assert length(s1.pending[@gb]) == 3
    end
  end

  describe "maybe_emit/2 — max_wait forced flush" do
    # When the max_wait timer fires, it sets `force_flush: true` in state.
    # maybe_emit should then flush the LRU group even when qlen > 0
    # (normally it would hold).

    test "with force_flush true and non-empty mailbox, flushes instead of holding" do
      pending = Enum.map(1..3, &entry/1)
      s0 = pending |> single_group(1, batch_size: 100) |> Map.put(:force_flush, true)

      # qlen=5 means mailbox is non-empty — normally holds. But
      # force_flush overrides.
      assert {[event], s1} = Producer.maybe_emit(s0, 5)
      assert {:flurry_batch, _, _, @g, entries} = event
      assert length(entries) == 3
      assert s1.force_flush == false
    end

    test "with force_flush false and non-empty mailbox, holds as usual" do
      pending = Enum.map(1..3, &entry/1)
      s0 = pending |> single_group(1, batch_size: 100) |> Map.put(:force_flush, false)

      assert {[], _s1} = Producer.maybe_emit(s0, 5)
    end

    test "force_flush is cleared after the flush" do
      s0 = [entry(1)] |> single_group(1) |> Map.put(:force_flush, true)
      {[_], s1} = Producer.maybe_emit(s0, 5)
      assert s1.force_flush == false
    end
  end

  describe "additive merging (pure helpers)" do
    # Tests for the pure helpers that split a group tuple into routing
    # + additive values, merge additive values across entries, and
    # reconstruct the full tuple.

    test "split_tuple/2 with no additive positions returns the tuple unchanged" do
      assert {{1, true}, []} = Producer.split_tuple({1, true}, [])
    end

    test "split_tuple/2 removes additive positions into a separate list" do
      # group args: [user_id, active?, preloads], additive positions: [2]
      assert {{1, true}, [[:posts]]} = Producer.split_tuple({1, true, [:posts]}, [2])
    end

    test "split_tuple/2 handles multiple additive positions" do
      # positions [0, 2] additive, position [1] routing
      assert {{:middle}, [[:a], [:b]]} =
               Producer.split_tuple({[:a], :middle, [:b]}, [0, 2])
    end

    test "merge_additive_values/2 returns [] when there are no additive positions" do
      assert [] = Producer.merge_additive_values([entry(1)], 0)
    end

    test "merge_additive_values/2 unions list values at each additive position" do
      entries = [
        entry(1, [[:posts]]),
        entry(2, [[:comments]]),
        entry(3, [[:posts, :profile]])
      ]

      assert [merged] = Producer.merge_additive_values(entries, 1)
      assert Enum.sort(merged) == [:comments, :posts, :profile]
    end

    test "merge_additive_values/2 merges multiple additive positions independently" do
      entries = [
        entry(1, [[:a], [:x]]),
        entry(2, [[:b], [:y]]),
        entry(3, [[:a, :c], [:y, :z]])
      ]

      assert [pos1, pos2] = Producer.merge_additive_values(entries, 2)
      assert Enum.sort(pos1) == [:a, :b, :c]
      assert Enum.sort(pos2) == [:x, :y, :z]
    end

    test "reconstruct_tuple/3 with no additive positions returns routing_key" do
      assert {1, true} = Producer.reconstruct_tuple({1, true}, [], [])
    end

    test "reconstruct_tuple/3 interleaves merged additive values back into original positions" do
      assert {1, true, [:posts]} =
               Producer.reconstruct_tuple({1, true}, [[:posts]], [2])
    end

    test "reconstruct_tuple/3 handles additive positions in the middle and at the start" do
      assert {[:x], :middle, [:y]} =
               Producer.reconstruct_tuple({:middle}, [[:x], [:y]], [0, 2])
    end

    test "split_tuple/2 + reconstruct_tuple/3 round-trips a single-entry case" do
      original = {1, :foo, [:posts]}
      {routing, additives} = Producer.split_tuple(original, [2])
      assert original == Producer.reconstruct_tuple(routing, additives, [2])
    end
  end
end
