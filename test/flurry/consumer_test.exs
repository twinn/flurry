defmodule Flurry.ConsumerTest.Row do
  @moduledoc false
  defstruct [:id, :name]
end

defmodule Flurry.ConsumerTest do
  # Pure-function tests of the result-correlation logic, which the consumer
  # calls on each batch. These don't need a GenStage pipeline running —
  # `correlate/3` takes a results list, a key field, and a mode, and returns
  # either a lookup map (`:one`) or a group map (`:list`).
  use ExUnit.Case, async: true

  alias Flurry.Consumer
  alias Flurry.ConsumerTest.Row

  doctest Consumer

  describe "correlate/3 — :one mode" do
    test "builds a lookup map keyed on the given field" do
      results = [%{id: 1, name: "a"}, %{id: 2, name: "b"}]

      assert Consumer.correlate(results, :id, :one) == %{
               1 => %{id: 1, name: "a"},
               2 => %{id: 2, name: "b"}
             }
    end

    test "raises AmbiguousBatchError on duplicate keys" do
      results = [%{id: 1, name: "a"}, %{id: 1, name: "b"}]

      assert_raise Flurry.AmbiguousBatchError, ~r/returns: :list/, fn ->
        Consumer.correlate(results, :id, :one)
      end
    end

    test "works with structs" do
      results = [%Row{id: 1, name: "a"}, %Row{id: 2, name: "b"}]
      map = Consumer.correlate(results, :id, :one)
      assert %Row{id: 1} = map[1]
      assert %Row{id: 2} = map[2]
    end

    test "empty input → empty map" do
      assert Consumer.correlate([], :id, :one) == %{}
    end
  end

  describe "correlate/3 — :list mode" do
    test "groups records by key" do
      results = [
        %{group: :a, id: 1},
        %{group: :a, id: 2},
        %{group: :b, id: 3}
      ]

      assert Consumer.correlate(results, :group, :list) == %{
               a: [%{group: :a, id: 1}, %{group: :a, id: 2}],
               b: [%{group: :b, id: 3}]
             }
    end

    test "empty input → empty map" do
      assert Consumer.correlate([], :group, :list) == %{}
    end

    test "does not raise on duplicate keys" do
      results = [%{group: :a, id: 1}, %{group: :a, id: 2}]
      assert %{a: [_, _]} = Consumer.correlate(results, :group, :list)
    end
  end

  describe "lookup/3 — matching caller args to correlated results" do
    test ":one mode returns the record or nil" do
      map = %{1 => %{id: 1, name: "a"}}
      assert Consumer.lookup(map, 1, :one) == %{id: 1, name: "a"}
      assert Consumer.lookup(map, 99, :one) == nil
    end

    test ":list mode returns the list or []" do
      map = %{a: [%{group: :a}]}
      assert Consumer.lookup(map, :a, :list) == [%{group: :a}]
      assert Consumer.lookup(map, :z, :list) == []
    end
  end

  describe "handle_failure/3 — pure strategy dispatch" do
    # These tests exercise the pure decision function that decides, given a
    # failed batch and a strategy, what to reply and what to requeue. They
    # don't need a GenStage pipeline.

    defp from(n), do: {self(), then(make_ref(), &{n, &1})}

    test ":fail_all with any size batch → reply error to every entry, no requeue" do
      e = %RuntimeError{message: "boom"}
      entries = [{1, from(1)}, {2, from(2)}, {3, from(3)}]

      assert {replies, requeues} = Consumer.handle_failure(:fail_all, entries, e)
      assert length(replies) == 3
      assert Enum.all?(replies, fn {_from, {:error, ^e}} -> true end)
      assert requeues == []
    end

    test ":bisect with a singleton entry → reply error to that one caller, no requeue" do
      e = %RuntimeError{message: "boom"}
      entries = [{8, from(8)}]

      assert {replies, requeues} = Consumer.handle_failure(:bisect, entries, e)
      assert [{_from, {:error, ^e}}] = replies
      assert requeues == []
    end

    test ":bisect with multi-entry batch → no replies, requeue two halves" do
      e = %RuntimeError{message: "boom"}
      entries = Enum.map(1..8, &{&1, from(&1)})

      assert {replies, requeues} = Consumer.handle_failure(:bisect, entries, e)
      assert replies == []
      assert [left, right] = requeues
      assert length(left) == 4
      assert length(right) == 4
      assert Enum.map(left, &elem(&1, 0)) == [1, 2, 3, 4]
      assert Enum.map(right, &elem(&1, 0)) == [5, 6, 7, 8]
    end

    test ":bisect with odd-length batch splits the larger half to the right" do
      e = %RuntimeError{message: "boom"}
      entries = Enum.map(1..7, &{&1, from(&1)})

      assert {[], [left, right]} = Consumer.handle_failure(:bisect, entries, e)
      assert length(left) == 3
      assert length(right) == 4
    end
  end

  describe "Flurry.BulkCallFailed exception" do
    test "carries :kind and :reason" do
      e = Flurry.BulkCallFailed.exception(kind: :exit, reason: {:timeout, {GenServer, :call, []}})
      assert e.kind == :exit
      assert match?({:timeout, _}, e.reason)
      assert e.message =~ "exit"
      assert e.message =~ "timeout"
    end
  end
end
