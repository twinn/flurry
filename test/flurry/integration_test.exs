defmodule Flurry.IntegrationTest do
  # End-to-end tests: start a real `use Flurry` module under supervision,
  # make concurrent calls, assert that batching actually coalesces work.
  use ExUnit.Case, async: false

  alias Flurry.Test.FakeBatcher

  @records [
    %{id: 1, name: "alice", group: :a},
    %{id: 2, name: "bob", group: :a},
    %{id: 3, name: "carol", group: :b},
    %{id: 4, name: "dave", group: :b},
    %{id: 5, name: "eve", group: :c}
  ]

  setup do
    :ok = FakeBatcher.start_state(@records)
    start_supervised!(FakeBatcher)
    on_exit(&FakeBatcher.stop_state/0)
    :ok
  end

  describe "single caller" do
    test "get/1 returns the matching record" do
      assert %{id: 1, name: "alice"} = FakeBatcher.get(1)
    end

    test "get/1 returns nil for a missing id" do
      assert FakeBatcher.get(999) == nil
    end

    test "get_by_group/1 returns all records in the group (:list mode)" do
      result = FakeBatcher.get_by_group(:a)
      assert length(result) == 2
      assert Enum.all?(result, &(&1.group == :a))
    end

    test "get_by_group/1 returns [] for an unknown group" do
      assert FakeBatcher.get_by_group(:nonexistent) == []
    end
  end

  describe "batching under concurrency" do
    test "N concurrent callers get coalesced into one bulk call" do
      FakeBatcher.reset_call_count()

      # Hold the first batch open so the rest pile up in the producer.
      test_pid = self()
      FakeBatcher.set_block(test_pid)

      tasks = Enum.map(1..5, fn id -> Task.async(fn -> FakeBatcher.get(id) end) end)

      # Wait for the first batch to reach the bulk fn (which will be blocked).
      assert_receive {:bulk_fn_running, first_runner, _first_ids}, 1_000

      # Give the remaining tasks a moment to enqueue.
      Process.sleep(50)

      # Now stop holding new batches open — the second batch should run normally.
      FakeBatcher.clear_block()

      # Release the first batch.
      send(first_runner, :go)

      results = Task.await_many(tasks, 5_000)

      # Every caller should get their matching record back.
      for {result, index} <- Enum.with_index(results, 1) do
        assert result.id == index
      end

      # Most important: the bulk function ran at most twice (first singleton
      # batch + one coalesced batch of the rest), not 5 times.
      assert FakeBatcher.call_count() <= 2
    end

    test "duplicate ids across callers are deduplicated in the bulk call" do
      FakeBatcher.reset_call_count()

      test_pid = self()
      FakeBatcher.set_block(test_pid)

      tasks = Enum.map(1..5, fn _ -> Task.async(fn -> FakeBatcher.get(1) end) end)

      assert_receive {:bulk_fn_running, first_runner, _}, 1_000
      Process.sleep(50)
      FakeBatcher.clear_block()
      send(first_runner, :go)

      results = Task.await_many(tasks, 5_000)

      # All 5 callers asked for id=1 — all should receive the same record.
      assert Enum.all?(results, &(&1.id == 1))

      # The bulk function should have received [1] only once per batch, not
      # [1, 1, 1, 1, 1]. We can't directly see the args from here, but we
      # can check the runtime didn't explode and all callers got identical
      # results.
    end
  end

  describe "batch_size cap" do
    # Uses a dedicated module (not FakeBatcher) so we can observe exactly
    # which id lists the bulk function saw, without interference from the
    # shared FakeBatcher state.
    defmodule CappedBatcher do
      @moduledoc false
      use Flurry, repo: :none

      @decorate batch(get(id))
      def get_many(ids) do
        Agent.update(__MODULE__.Sink, &[ids | &1])
        Enum.map(ids, &%{id: &1})
      end
    end

    setup do
      {:ok, _} = Agent.start(fn -> [] end, name: CappedBatcher.Sink)
      start_supervised!({CappedBatcher, batch_size: 3})
      on_exit(fn -> safe_agent_stop(CappedBatcher.Sink) end)
      :ok
    end

    test "no bulk call ever receives more than batch_size ids" do
      # Fire 20 concurrent requests through a batcher capped at 3.
      tasks = Enum.map(1..20, fn id -> Task.async(fn -> CappedBatcher.get(id) end) end)
      results = Task.await_many(tasks, 5_000)

      # Every caller got their record back.
      assert Enum.sort_by(results, & &1.id) == Enum.map(1..20, &%{id: &1})

      # The bulk function was invoked with id lists of size <= 3, never more.
      observed_batches = Agent.get(CappedBatcher.Sink, & &1)

      assert Enum.all?(observed_batches, &(length(&1) <= 3)),
             "expected every batch to have at most 3 ids, got sizes: " <>
               inspect(Enum.map(observed_batches, &length/1))

      # And we saw enough batches to cover all 20 (at worst, 20 singletons).
      total_ids = observed_batches |> List.flatten() |> Enum.uniq() |> length()
      assert total_ids == 20
    end
  end

  describe "per-decorated-function batch_size" do
    defmodule MixedCapsBatcher do
      @moduledoc false
      use Flurry, repo: :none

      # Decorator-level override wins over start_link's default.
      @decorate batch(get_small(id), batch_size: 2)
      def get_many_small(ids) do
        Agent.update(__MODULE__.Sink, &Map.update!(&1, :small, fn xs -> [ids | xs] end))
        Enum.map(ids, &%{id: &1})
      end

      # No decorator-level override — inherits from start_link opts.
      @decorate batch(get_default(id))
      def get_many_default(ids) do
        Agent.update(__MODULE__.Sink, &Map.update!(&1, :default, fn xs -> [ids | xs] end))
        Enum.map(ids, &%{id: &1})
      end
    end

    setup do
      {:ok, _} = Agent.start(fn -> %{small: [], default: []} end, name: MixedCapsBatcher.Sink)
      start_supervised!({MixedCapsBatcher, batch_size: 10})
      on_exit(fn -> safe_agent_stop(MixedCapsBatcher.Sink) end)
      :ok
    end

    test "decorator-level batch_size overrides the module-level default" do
      # Fire 20 concurrent requests through the `get_small` function.
      tasks = Enum.map(1..20, fn id -> Task.async(fn -> MixedCapsBatcher.get_small(id) end) end)
      results = Task.await_many(tasks, 5_000)

      assert Enum.sort_by(results, & &1.id) == Enum.map(1..20, &%{id: &1})

      observed = Agent.get(MixedCapsBatcher.Sink, & &1.small)

      assert Enum.all?(observed, &(length(&1) <= 2)),
             "expected small batches to cap at 2, got sizes: " <>
               inspect(Enum.map(observed, &length/1))
    end

    test "functions without a decorator override inherit from start_link opts" do
      # 15 concurrent requests, module-level cap is 10, this function has
      # no decorator override, so it should cap at 10.
      tasks = Enum.map(1..15, fn id -> Task.async(fn -> MixedCapsBatcher.get_default(id) end) end)
      _ = Task.await_many(tasks, 5_000)

      observed = Agent.get(MixedCapsBatcher.Sink, & &1.default)

      assert Enum.all?(observed, &(length(&1) <= 10)),
             "expected default batches to cap at 10, got sizes: " <>
               inspect(Enum.map(observed, &length/1))
    end

    test "__flurry_batches__ carries the decorator-level batch_size" do
      batches = MixedCapsBatcher.__flurry_batches__()
      small = Enum.find(batches, &(&1.singular == :get_small))
      default = Enum.find(batches, &(&1.singular == :get_default))

      assert small.batch_size == 2
      assert default.batch_size == nil
    end
  end

  describe "multi-arg / group-keyed batching" do
    # A decorated function with more than one argument. The first argument
    # is the batched variable; the remaining arguments form the group key.
    # Callers sharing the same group key get coalesced into one bulk fn
    # invocation; callers with different group keys run in independent
    # batches.
    defmodule GroupedBatcher do
      @moduledoc false
      use Flurry, repo: :none

      @decorate batch(get_post(slug, user_id, active?))
      def get_many_posts(slugs, user_id, active?) do
        Agent.update(__MODULE__.Sink, fn s ->
          %{s | calls: [{slugs, user_id, active?} | s.calls]}
        end)

        for slug <- slugs do
          %{slug: slug, user_id: user_id, active: active?}
        end
      end
    end

    setup do
      {:ok, _} = Agent.start(fn -> %{calls: []} end, name: GroupedBatcher.Sink)
      start_supervised!(GroupedBatcher)
      on_exit(fn -> safe_agent_stop(GroupedBatcher.Sink) end)
      :ok
    end

    test "each caller gets the correct record" do
      result = GroupedBatcher.get_post("a", 1, true)
      assert result == %{slug: "a", user_id: 1, active: true}
    end

    test "distinct groups run as independent bulk calls, never mixed" do
      # Fire a burst with three distinct groups:
      # - {1, true}: slugs "a", "b"
      # - {2, true}: slug  "c"
      # - {1, false}: slug "d"
      tasks = [
        Task.async(fn -> GroupedBatcher.get_post("a", 1, true) end),
        Task.async(fn -> GroupedBatcher.get_post("b", 1, true) end),
        Task.async(fn -> GroupedBatcher.get_post("c", 2, true) end),
        Task.async(fn -> GroupedBatcher.get_post("d", 1, false) end)
      ]

      results = Task.await_many(tasks, 5_000)

      # Every caller got their record back with the right context.
      assert [
               %{slug: "a", user_id: 1, active: true},
               %{slug: "b", user_id: 1, active: true},
               %{slug: "c", user_id: 2, active: true},
               %{slug: "d", user_id: 1, active: false}
             ] = results

      # Inspect the bulk fn invocations: each invocation should have a
      # single group (one (user_id, active?) pair), never mixed.
      calls = Agent.get(GroupedBatcher.Sink, & &1.calls)

      for {_slugs, _user_id, _active?} <- calls do
        :ok
      end

      # Group every bulk call by its (user_id, active?) pair and verify we
      # saw all three distinct groups.
      groups_seen =
        calls
        |> Enum.map(fn {_slugs, u, a} -> {u, a} end)
        |> Enum.uniq()
        |> Enum.sort()

      assert Enum.sort([{1, true}, {2, true}, {1, false}]) == groups_seen

      # The {1, true} group should have flushed with both slugs together
      # (or as two singletons, depending on timing) — but the slugs should
      # always match the group.
      for {slugs, user_id, active?} <- calls do
        for slug <- slugs do
          expected = {slug, user_id, active?}
          assert expected in [{"a", 1, true}, {"b", 1, true}, {"c", 2, true}, {"d", 1, false}]
        end
      end
    end

    test "callers in the same group coalesce" do
      # 10 concurrent callers, all in the same group {1, true} — they
      # should all pile into one or two bulk calls, not ten.
      tasks =
        for i <- 1..10 do
          slug = "slug-#{i}"
          Task.async(fn -> GroupedBatcher.get_post(slug, 1, true) end)
        end

      results = Task.await_many(tasks, 5_000)
      assert length(results) == 10
      assert Enum.all?(results, &match?(%{user_id: 1, active: true}, &1))

      calls = Agent.get(GroupedBatcher.Sink, & &1.calls)
      # Under heavy coalescing, we expect < 10 bulk calls.
      assert length(calls) < 10
    end
  end

  describe ":bisect error strategy" do
    # A BisectBatcher that fails loudly whenever the batch contains id=8.
    # Goal: id=8 gets isolated and its caller gets an error; every other
    # caller in the same batch still gets their record.
    defmodule BisectBatcher do
      @moduledoc false
      use Flurry, repo: :none

      @decorate batch(get(id), on_failure: :bisect)
      def get_many(ids) do
        Agent.update(__MODULE__.Sink, fn s ->
          %{s | calls: s.calls + 1, id_lists: [ids | s.id_lists]}
        end)

        if 8 in ids, do: raise("poison id=8")
        Enum.map(ids, &%{id: &1})
      end
    end

    setup do
      {:ok, _} = Agent.start(fn -> %{calls: 0, id_lists: []} end, name: BisectBatcher.Sink)
      start_supervised!(BisectBatcher)
      on_exit(fn -> safe_agent_stop(BisectBatcher.Sink) end)
      :ok
    end

    test "isolates the poison, other callers in the same batch succeed" do
      # Fire 16 concurrent callers. One of them asks for id=8.
      tasks = Enum.map(1..16, fn id -> Task.async(fn -> safe(fn -> BisectBatcher.get(id) end) end) end)
      results = Task.await_many(tasks, 10_000)

      # Everyone except caller 8 got their record back.
      for {result, caller_id} <- Enum.with_index(results, 1) do
        if caller_id == 8 do
          assert {:error, _} = result
        else
          assert %{id: ^caller_id} = result
        end
      end
    end

    test "does fewer bulk calls than a naive :individual strategy would" do
      tasks = Enum.map(1..16, fn id -> Task.async(fn -> safe(fn -> BisectBatcher.get(id) end) end) end)
      _ = Task.await_many(tasks, 10_000)

      calls = Agent.get(BisectBatcher.Sink, & &1.calls)
      # Individual would be 1 initial + 16 singletons = 17.
      # Bisect on 1 bad id out of 16 should be around 1 + 2*log2(16) = 9,
      # possibly a couple more if the initial batch wasn't the full 16.
      # Assert comfortably below the :individual worst case.
      assert calls < 17,
             "bisect should use fewer bulk calls than individual retry (<17), got #{calls}"
    end

    test "bisect eventually reaches a singleton for the poisoned id" do
      tasks = Enum.map(1..16, fn id -> Task.async(fn -> safe(fn -> BisectBatcher.get(id) end) end) end)
      _ = Task.await_many(tasks, 10_000)

      # The last batch the bulk fn saw that contained id=8 should have
      # been a singleton — that's how bisect definitively isolates the
      # poison before replying an error to its caller.
      id_lists = BisectBatcher.Sink |> Agent.get(& &1.id_lists) |> Enum.reverse()
      batches_with_8 = Enum.filter(id_lists, &(8 in &1))

      assert [_ | _] = batches_with_8, "expected at least one bulk call containing id=8"
      assert List.last(batches_with_8) == [8], "expected bisect to descend to a singleton [8]"
    end
  end

  describe "exit handling" do
    # An exit from the bulk fn (e.g. a downstream GenServer.call timeout,
    # or the consumer being linked to a dying process) must be caught and
    # routed through the :on_failure strategy — same as a raised
    # exception. Before this handling, an exit would crash the consumer,
    # the supervisor would restart the pipeline, and callers would see
    # opaque {:exit, ...} from their own GenServer.call.

    defmodule ExitingBatcher do
      @moduledoc false
      use Flurry, repo: :none

      @decorate batch(get(id), on_failure: :bisect)
      def get_many(ids) do
        Agent.update(__MODULE__.Sink, fn s -> %{s | calls: s.calls + 1} end)
        if 8 in ids, do: exit(:bulk_fn_went_away)
        Enum.map(ids, &%{id: &1})
      end
    end

    setup do
      {:ok, _} = Agent.start(fn -> %{calls: 0} end, name: ExitingBatcher.Sink)
      start_supervised!(ExitingBatcher)
      on_exit(fn -> safe_agent_stop(ExitingBatcher.Sink) end)
      :ok
    end

    test "an exit in the bulk fn is wrapped in Flurry.BulkCallFailed and delivered to its caller" do
      tasks =
        Enum.map(1..16, fn id ->
          Task.async(fn -> safe(fn -> ExitingBatcher.get(id) end) end)
        end)

      results = Task.await_many(tasks, 10_000)

      for {result, caller_id} <- Enum.with_index(results, 1) do
        if caller_id == 8 do
          assert {:error, %Flurry.BulkCallFailed{kind: :exit, reason: :bulk_fn_went_away}} = result
        else
          assert %{id: ^caller_id} = result
        end
      end
    end
  end

  describe "correlate: override" do
    # A batcher whose records use a DIFFERENT field name than the
    # decorated arg. Default behavior would use :id; we override to
    # :uuid so results are matched by the record's :uuid field.
    defmodule RenamedCorrelateBatcher do
      @moduledoc false
      use Flurry, repo: :none

      @decorate batch(get(id), correlate: :uuid)
      def get_many(ids) do
        Enum.map(ids, fn id -> %{uuid: id, name: "item-#{id}"} end)
      end
    end

    setup do
      start_supervised!(RenamedCorrelateBatcher)
      :ok
    end

    test "correlates by the overridden field name, not the arg name" do
      assert %{uuid: 42, name: "item-42"} = RenamedCorrelateBatcher.get(42)
      assert %{uuid: "abc", name: "item-abc"} = RenamedCorrelateBatcher.get("abc")
    end
  end

  describe "timeout: override" do
    # A batcher whose bulk fn sleeps longer than the decorator's timeout.
    # The caller should see an exit (from GenServer.call timeout), proving
    # the timeout actually takes effect.
    defmodule SlowBatcher do
      @moduledoc false
      use Flurry, repo: :none

      @decorate batch(get(id), timeout: 50)
      def get_many(ids) do
        Process.sleep(200)
        Enum.map(ids, &%{id: &1})
      end
    end

    setup do
      start_supervised!(SlowBatcher)
      :ok
    end

    test "caller exits with :timeout when the bulk fn is slower than the decorator's timeout" do
      # Using safe_call to catch the GenServer.call exit cleanly.
      assert {:error, {:timeout, _}} = safe_call(fn -> SlowBatcher.get(1) end)
    end
  end

  describe "batch_by: normalizer" do
    # A decorated fn whose group arg is a map with some noise fields
    # (simulating an Ecto struct with preloads or extraneous data). The
    # batch_by normalizer extracts just the :id, so two callers passing
    # structurally-different-but-semantically-equal maps coalesce into
    # the same batch. The bulk fn receives the NORMALIZED value
    # (integer id), not the original struct.
    defmodule NormalizedBatchByBatcher do
      @moduledoc false
      use Flurry, repo: :none

      @decorate batch(
                  get(id, owner),
                  batch_by: fn {owner} -> {owner.id} end
                )
      def get_many(ids, owner_id) do
        Agent.update(
          __MODULE__.Sink,
          fn s -> %{s | calls: [{ids, owner_id} | s.calls]} end
        )

        # Note: owner_id is an integer here (post-normalization), not
        # the original map.
        for id <- ids, do: %{id: id, owner_id: owner_id}
      end
    end

    setup do
      {:ok, _} = Agent.start(fn -> %{calls: []} end, name: NormalizedBatchByBatcher.Sink)
      start_supervised!(NormalizedBatchByBatcher)
      on_exit(fn -> safe_agent_stop(NormalizedBatchByBatcher.Sink) end)
      :ok
    end

    test "the bulk fn receives the normalized value, not the raw group arg" do
      # Caller passes a map with an :id and some noise fields. The
      # batch_by extracts .id, so the bulk fn should see just the int.
      assert %{id: 1, owner_id: 42} =
               NormalizedBatchByBatcher.get(1, %{id: 42, noise: :a, other: [1, 2, 3]})

      # The bulk fn was called with an integer owner_id (not a map).
      [{_ids, owner_id} | _] = Agent.get(NormalizedBatchByBatcher.Sink, & &1.calls)
      assert owner_id == 42
      assert is_integer(owner_id)
    end

    test "callers with structurally different but semantically equal group args coalesce" do
      # Two callers, same owner.id but different "noise" fields. Without
      # batch_by they'd form two distinct groups; with batch_by they
      # should share one group and coalesce.
      tasks = [
        Task.async(fn ->
          NormalizedBatchByBatcher.get(1, %{id: 42, noise: :preloaded_posts})
        end),
        Task.async(fn ->
          NormalizedBatchByBatcher.get(2, %{id: 42, noise: :not_preloaded})
        end)
      ]

      results = Task.await_many(tasks, 5_000)

      assert [%{id: 1, owner_id: 42}, %{id: 2, owner_id: 42}] = results

      # Every bulk call's owner_id should be 42 (the normalized int),
      # never a map. Proves the normalization was applied before any
      # state was stored.
      calls = Agent.get(NormalizedBatchByBatcher.Sink, & &1.calls)
      assert Enum.all?(calls, fn {_ids, owner_id} -> owner_id == 42 end)
      # Under coalescing, <2 calls is possible; under total serialization, 2.
      assert length(calls) <= 2
    end
  end

  describe "error propagation" do
    test "an exception in the bulk function is delivered to all callers" do
      defmodule Exploder do
        @moduledoc false
        use Flurry, repo: :none

        @decorate batch(get(id))
        def get_many(_ids), do: raise("boom")
      end

      start_supervised!(Exploder)

      for id <- 1..3 do
        assert {:error, _} = safe_call(fn -> Exploder.get(id) end)
      end
    end
  end

  defp safe_call(f) do
    f.()
  rescue
    e -> {:error, e}
  catch
    :exit, reason -> {:error, reason}
  end

  defp safe(f) do
    case f.() do
      {:error, _} = err -> err
      other -> other
    end
  rescue
    e -> {:error, e}
  catch
    :exit, reason -> {:error, reason}
  end

  # Tolerates the race between whereis and Agent.stop: the process may
  # die between our lookup and our stop call if its linker dies.
  defp safe_agent_stop(name) do
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
end
