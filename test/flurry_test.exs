defmodule FlurryTest do
  use ExUnit.Case, async: true

  alias Flurry.Test.FakeBatcher

  setup_all do
    Code.ensure_loaded!(FakeBatcher)
    :ok
  end

  describe "use Flurry registration" do
    test "exposes __flurry_batches__/0 with the registered decorations" do
      batches = FakeBatcher.__flurry_batches__()
      assert is_list(batches)
      assert length(batches) == 2

      get_batch = Enum.find(batches, &(&1.singular == :get))
      assert get_batch.key == :id
      assert get_batch.bulk == :get_many
      assert get_batch.returns == :one

      group_batch = Enum.find(batches, &(&1.singular == :get_by_group))
      assert group_batch.key == :group
      assert group_batch.bulk == :get_many_by_group
      assert group_batch.returns == :list
    end

    test "generates the singular entry-point functions" do
      assert function_exported?(FakeBatcher, :get, 1)
      assert function_exported?(FakeBatcher, :get_by_group, 1)
    end

    test "generates child_spec/1 and start_link/1" do
      assert function_exported?(FakeBatcher, :child_spec, 1)
      assert function_exported?(FakeBatcher, :start_link, 1)
    end

    test "leaves the user-defined bulk functions callable" do
      assert function_exported?(FakeBatcher, :get_many, 1)
      assert function_exported?(FakeBatcher, :get_many_by_group, 1)
    end

    test "batch_size defaults to nil when not specified in the decorator" do
      batches = FakeBatcher.__flurry_batches__()
      assert Enum.all?(batches, &(&1.batch_size == nil))
    end

    test "on_failure defaults to :bisect when not specified in the decorator" do
      batches = FakeBatcher.__flurry_batches__()
      assert Enum.all?(batches, &(&1.on_failure == :bisect))
    end

    test "generated singular functions have @spec attached" do
      # Specs are stored in the compiled beam; fetch_specs returns
      # [{{name, arity}, [spec_ast]}, ...].
      {:ok, specs} = Code.Typespec.fetch_specs(FakeBatcher)
      spec_map = Map.new(specs)

      assert Map.has_key?(spec_map, {:get, 1})
      assert Map.has_key?(spec_map, {:get_by_group, 1})
      assert Map.has_key?(spec_map, {:start_link, 1})
      assert Map.has_key?(spec_map, {:child_spec, 1})
      assert Map.has_key?(spec_map, {:__flurry_batches__, 0})
    end
  end

  describe "decorator validation" do
    test "invalid :returns raises at compile time" do
      ast =
        quote do
          defmodule BadReturns do
            @moduledoc false
            use Flurry, repo: :none

            @decorate batch(get(id), returns: :wrong)
            def get_many(ids), do: ids
          end
        end

      assert_raise ArgumentError, ~r/invalid `:returns`/, fn ->
        Code.eval_quoted(ast)
      end
    end

    test "invalid :batch_size raises at compile time" do
      ast =
        quote do
          defmodule BadBatchSize do
            @moduledoc false
            use Flurry, repo: :none

            @decorate batch(get(id), batch_size: -1)
            def get_many(ids), do: ids
          end
        end

      assert_raise ArgumentError, ~r/invalid `:batch_size`/, fn ->
        Code.eval_quoted(ast)
      end
    end

    test "invalid :on_failure raises at compile time" do
      ast =
        quote do
          defmodule BadOnFailure do
            @moduledoc false
            use Flurry, repo: :none

            @decorate batch(get(id), on_failure: :retry_forever)
            def get_many(ids), do: ids
          end
        end

      assert_raise ArgumentError, ~r/invalid `:on_failure`/, fn ->
        Code.eval_quoted(ast)
      end
    end

    test "use Flurry without :repo raises at compile time" do
      ast =
        quote do
          defmodule MissingRepo do
            @moduledoc false
            use Flurry

            @decorate batch(get(id))
            def get_many(ids), do: ids
          end
        end

      assert_raise ArgumentError, ~r/requires a `:repo` option/, fn ->
        Code.eval_quoted(ast)
      end
    end

    test "invalid :in_transaction value raises at compile time" do
      ast =
        quote do
          defmodule BadInTransaction do
            @moduledoc false
            use Flurry, repo: :none

            @decorate batch(get(id), in_transaction: :nope)
            def get_many(ids), do: ids
          end
        end

      assert_raise ArgumentError, ~r/invalid `:in_transaction`/, fn ->
        Code.eval_quoted(ast)
      end
    end

    test ":warn with repo: :none raises at compile time" do
      ast =
        quote do
          defmodule WarnWithoutRepo do
            @moduledoc false
            use Flurry, repo: :none

            @decorate batch(get(id), in_transaction: :warn)
            def get_many(ids), do: ids
          end
        end

      assert_raise ArgumentError, ~r/repo: :none.*require a real `:repo`/s, fn ->
        Code.eval_quoted(ast)
      end
    end

    test ":bypass with repo: :none raises at compile time" do
      ast =
        quote do
          defmodule BypassWithoutRepo do
            @moduledoc false
            use Flurry, repo: :none

            @decorate batch(get(id), in_transaction: :bypass)
            def get_many(ids), do: ids
          end
        end

      assert_raise ArgumentError, ~r/repo: :none.*require a real `:repo`/s, fn ->
        Code.eval_quoted(ast)
      end
    end
  end

  describe "in_transaction default resolution" do
    defmodule DefaultsNone do
      @moduledoc false
      use Flurry, repo: :none

      @decorate batch(get(id))
      def get_many(ids), do: Enum.map(ids, &%{id: &1})
    end

    defmodule FakeRepo do
      @moduledoc false
      def checked_out?, do: false
    end

    defmodule DefaultsRealRepo do
      @moduledoc false
      use Flurry, repo: FlurryTest.FakeRepo

      @decorate batch(get(id))
      def get_many(ids), do: Enum.map(ids, &%{id: &1})
    end

    test "default is :safe when repo is :none" do
      [batch] = DefaultsNone.__flurry_batches__()
      assert batch.in_transaction == :safe
    end

    test "default is :warn when repo is a real module" do
      [batch] = DefaultsRealRepo.__flurry_batches__()
      assert batch.in_transaction == :warn
    end
  end
end
