defmodule Flurry.Consumer do
  @moduledoc false
  # GenStage consumer that runs the user's bulk function on each batch event
  # and replies to each captured caller with their correlated result.
  #
  # Subscribes with `max_demand: 1, min_demand: 0`: one batch event is in
  # flight at a time. This lets the producer accumulate a new batch in
  # parallel while the current one is executing.

  use GenStage

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts, name: opts[:name])
  end

  @impl true
  def init(opts) do
    state = %{
      module: opts[:module],
      batch: opts[:batch],
      producer: opts[:producer]
    }

    {:consumer, state, subscribe_to: [{opts[:producer], max_demand: 1, min_demand: 0}]}
  end

  @impl true
  def handle_events(events, _from, state) do
    Enum.each(events, &process_batch(&1, state))
    {:noreply, [], state}
  end

  defp process_batch({:flurry_batch, module, batch, group_key, entries}, state) do
    args = entries |> Enum.map(fn {arg, _from} -> arg end) |> Enum.uniq()
    # The group key is the tuple of non-batched args captured at the call
    # site; splat it into the bulk fn after the batched list.
    bulk_args = [args | Tuple.to_list(group_key)]

    try do
      results = apply(module, batch.bulk, bulk_args)
      correlated = correlate(results, batch.correlate, batch.returns)

      Enum.each(entries, fn {arg, from} ->
        GenServer.reply(from, lookup(correlated, arg, batch.returns))
      end)
    rescue
      # Rescued exceptions pass through raw so users can pattern-match on
      # their own domain exception types (Postgrex.Error, Ecto.*, etc).
      e -> dispatch_failure(batch.on_failure, group_key, entries, state, e)
    catch
      # Exits have no natural exception shape — wrap in BulkCallFailed so
      # callers always see a struct.
      :exit, reason ->
        dispatch_failure(
          batch.on_failure,
          group_key,
          entries,
          state,
          Flurry.BulkCallFailed.exception(kind: :exit, reason: reason)
        )
    end
  end

  defp dispatch_failure(strategy, group_key, entries, state, e) do
    {replies, requeues} = handle_failure(strategy, entries, e)

    Enum.each(replies, fn {from, result} ->
      GenServer.reply(from, result)
    end)

    if requeues != [] do
      GenStage.cast(state.producer, {:requeue_batches, group_key, requeues})
    end
  end

  @doc """
  Pure strategy dispatch for a failed batch.

  Given the configured `:on_error` strategy, the failed batch's `entries`,
  and the raised exception, returns `{replies, requeues}` where:

    * `replies` is a list of `{from, result}` pairs to deliver via
      `GenServer.reply/2`.
    * `requeues` is a list of pre-formed batches (each a list of
      `{arg, from}` entries) to hand back to the producer as priority
      items for re-execution.

  Strategies:

    * `:fail_all` — every caller in the failed batch gets `{:error, e}`.
      No requeue.
    * `:bisect` — if the batch has more than one entry, split in half and
      requeue both; no replies yet. If the batch is a singleton, the
      failure is definitively isolated to that one caller — reply error
      to them alone.
  """
  @spec handle_failure(:fail_all | :bisect, [{term, GenServer.from()}], Exception.t()) ::
          {[{GenServer.from(), term}], [[{term, GenServer.from()}]]}
  def handle_failure(:fail_all, entries, e) do
    replies = Enum.map(entries, fn {_arg, from} -> {from, {:error, e}} end)
    {replies, []}
  end

  def handle_failure(:bisect, [{_arg, from}], e) do
    {[{from, {:error, e}}], []}
  end

  def handle_failure(:bisect, entries, _e) do
    mid = div(length(entries), 2)
    {left, right} = Enum.split(entries, mid)
    {[], [left, right]}
  end

  @doc """
  Builds the correlation map for a batch result set.

  * `:one` mode → `%{key_value => record}`. Raises `AmbiguousBatchError` if
    two records share the same key value.
  * `:list` mode → `%{key_value => [record, ...]}` grouping all records with
    the same key value.

  ## Examples

      iex> Flurry.Consumer.correlate([%{id: 1, name: "a"}, %{id: 2, name: "b"}], :id, :one)
      %{1 => %{id: 1, name: "a"}, 2 => %{id: 2, name: "b"}}

      iex> Flurry.Consumer.correlate([], :id, :one)
      %{}

      iex> Flurry.Consumer.correlate(
      ...>   [%{group: :a, id: 1}, %{group: :a, id: 2}, %{group: :b, id: 3}],
      ...>   :group,
      ...>   :list
      ...> )
      %{a: [%{group: :a, id: 1}, %{group: :a, id: 2}], b: [%{group: :b, id: 3}]}
  """
  @spec correlate([map() | struct()], atom(), :one | :list) :: map()
  def correlate(results, key, :one) do
    Enum.reduce(results, %{}, fn record, acc ->
      k = Map.fetch!(record, key)

      if Map.has_key?(acc, k) do
        raise Flurry.AmbiguousBatchError,
          message: ambiguous_message(key, k)
      end

      Map.put(acc, k, record)
    end)
  end

  def correlate(results, key, :list) do
    Enum.group_by(results, &Map.fetch!(&1, key))
  end

  @doc """
  Extracts one caller's result from the correlation map.

  ## Examples

      iex> Flurry.Consumer.lookup(%{1 => %{id: 1, name: "a"}}, 1, :one)
      %{id: 1, name: "a"}

      iex> Flurry.Consumer.lookup(%{1 => %{id: 1}}, 99, :one)
      nil

      iex> Flurry.Consumer.lookup(%{a: [%{id: 1}, %{id: 2}]}, :a, :list)
      [%{id: 1}, %{id: 2}]

      iex> Flurry.Consumer.lookup(%{}, :missing, :list)
      []
  """
  @spec lookup(map(), term(), :one | :list) :: term()
  def lookup(map, arg, :one), do: Map.get(map, arg)
  def lookup(map, arg, :list), do: Map.get(map, arg, [])

  defp ambiguous_message(key, value) do
    """
    Flurry: batch function returned multiple records with #{inspect(key)}=#{inspect(value)}.

    If this function is expected to return many records per key, declare it \
    with `returns: :list`:

        @decorate batch(my_fn(#{key}), returns: :list)
    """
  end
end
