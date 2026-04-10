defmodule Flurry.Producer do
  @moduledoc false
  # A GenStage producer that buffers individual requests and emits them as
  # a single batch event per flush. Flush policy:
  #
  #   * any group's pending >= batch_size → flush that group
  #   * mailbox empty → flush the least-recently-used non-empty group
  #   * max_wait timer fires → force-flush the LRU group
  #   * otherwise → hold, we expect more requests
  #
  # The "mailbox empty" check uses `Process.info(self(), :message_queue_len)`
  # and runs inside GenStage callbacks, i.e. at the moment we are about to
  # return from `handle_call` / `handle_demand`. A zero length at that moment
  # means "no further requests are immediately queued for us" — it's safe to
  # flush now instead of holding.
  #
  # The max_wait timer starts when the first entry arrives in an empty
  # producer. When it fires, `force_flush` is set so `maybe_emit` flushes
  # regardless of mailbox state. The timer is cancelled when all groups
  # drain, and restarted if entries remain after a forced flush.
  #
  # ## Groups
  #
  # When a decorated function has multiple arguments, the first argument is
  # batched and the remaining arguments form a group key. Entries are
  # partitioned by group key: callers sharing the same group get coalesced
  # into the same batch, callers with different groups get separate batches.
  # Each group has its own pending list, its own batch_size cap, and its own
  # slot in the LRU rotation. Single-arg decorations use `{}` as the
  # universal group key and behave as a single-group producer.

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
      batch_size: opts[:batch_size] || 100,
      max_wait: opts[:max_wait],
      flush_timer: nil,
      force_flush: false,
      pending: %{},
      group_order: [],
      priority: [],
      demand: 0
    }

    {:producer, state}
  end

  @impl true
  def handle_call({:enqueue, group_tuple, arg}, from, state) do
    # Split the group tuple into (routing_key, additive_values). For
    # non-additive batches, additive_positions is [], routing_key equals
    # the full tuple, and additive_values is []. The entry always stores
    # the 3-tuple form {arg, additive_values, from} — at emission time
    # we strip it back to {arg, from} for the event.
    {routing_key, additive_values} = split_tuple(group_tuple, state.batch.additive_positions)
    was_empty = map_size(state.pending) == 0
    state = enqueue(state, routing_key, arg, additive_values, from)
    {events, state} = do_maybe_emit(state)
    state = manage_flush_timer(state, was_empty)
    {:noreply, events, state}
  end

  @impl true
  def handle_demand(incoming, state) do
    state = %{state | demand: state.demand + incoming}
    {events, state} = do_maybe_emit(state)
    {:noreply, events, state}
  end

  @impl true
  def handle_cast({:requeue_batches, group_key, batches}, state) do
    # Append to the end — FIFO across multiple bisect rounds. Each element
    # of `batches` is a list of `{arg, from}` entries; each becomes its own
    # priority slot tagged with `group_key` and will be emitted as its own
    # single event.
    tagged = Enum.map(batches, fn entries -> {group_key, entries} end)
    state = %{state | priority: state.priority ++ tagged}
    {events, state} = do_maybe_emit(state)
    {:noreply, events, state}
  end

  @impl true
  def handle_info(:max_wait_flush, state) do
    state = %{state | flush_timer: nil, force_flush: true}
    {events, state} = do_maybe_emit(state)
    state = manage_flush_timer(state, false)
    {:noreply, events, state}
  end

  def handle_info(_msg, state), do: {:noreply, [], state}

  # Starts, cancels, or leaves the flush timer alone based on pending state.
  # Called after enqueue and after max_wait_flush fires.
  defp manage_flush_timer(%{max_wait: nil} = state, _was_empty), do: state

  defp manage_flush_timer(%{max_wait: max_wait} = state, was_empty) do
    cond do
      # All groups drained — cancel any running timer.
      map_size(state.pending) == 0 ->
        cancel_flush_timer(state)

      # First entry into an empty producer — start the timer.
      was_empty and state.flush_timer == nil ->
        ref = Process.send_after(self(), :max_wait_flush, max_wait)
        %{state | flush_timer: ref}

      # Timer already running or wasn't empty before — leave it.
      true ->
        state
    end
  end

  defp cancel_flush_timer(%{flush_timer: nil} = state), do: state

  defp cancel_flush_timer(%{flush_timer: ref} = state) do
    Process.cancel_timer(ref)
    %{state | flush_timer: nil}
  end

  defp enqueue(state, routing_key, arg, additive_values, from) do
    existing = Map.get(state.pending, routing_key, [])
    new_list = existing ++ [{arg, additive_values, from}]
    new_pending = Map.put(state.pending, routing_key, new_list)

    new_group_order =
      if Map.has_key?(state.pending, routing_key),
        do: state.group_order,
        else: state.group_order ++ [routing_key]

    %{state | pending: new_pending, group_order: new_group_order}
  end

  defp do_maybe_emit(state) do
    {:message_queue_len, qlen} = Process.info(self(), :message_queue_len)
    maybe_emit(state, qlen)
  end

  @doc """
  Determines whether the producer should emit a batch event given the
  current state and mailbox queue length.

  This function is extracted from the GenStage callbacks so that the
  flush policy can be exercised deterministically in unit tests without
  coordinating real mailbox state.

  Priority batches (pre-formed by the consumer via bisect) take precedence
  over pending entries. The priority queue is drained one batch at a time
  before emitting anything from pending.

  Pending emission selects a group by:

    1. Scanning for any group whose pending count has reached `batch_size`.
    2. If the mailbox is empty, selecting the front (LRU) of `group_order`.
    3. Otherwise, holding.
  """
  @spec maybe_emit(map(), non_neg_integer()) :: {list(), map()}
  def maybe_emit(%{demand: 0} = state, _qlen), do: {[], state}

  # Priority queue wins — emit first priority batch as-is, untouched.
  def maybe_emit(%{priority: [{group_key, batch} | rest]} = state, _qlen) do
    event = {:flurry_batch, state.module, state.batch, group_key, batch}
    {[event], %{state | priority: rest, demand: state.demand - 1}}
  end

  def maybe_emit(%{pending: pending} = state, _qlen) when map_size(pending) == 0 do
    {[], state}
  end

  def maybe_emit(state, qlen) do
    saturated = find_saturated(state)
    force = Map.get(state, :force_flush, false)

    cond do
      saturated != nil -> flush_group(state, saturated)
      qlen == 0 or force -> flush_group(%{state | force_flush: false}, hd(state.group_order))
      true -> {[], state}
    end
  end

  defp find_saturated(state) do
    Enum.find(state.group_order, fn gk ->
      length(Map.get(state.pending, gk, [])) >= state.batch_size
    end)
  end

  defp flush_group(state, routing_key) do
    entries = Map.get(state.pending, routing_key, [])
    {to_flush, remaining} = Enum.split(entries, state.batch_size)

    additive_positions = state.batch.additive_positions
    merged_additives = merge_additive_values(to_flush, length(additive_positions))
    full_group_tuple = reconstruct_tuple(routing_key, merged_additives, additive_positions)

    # Strip entries to the 2-tuple form the consumer expects in events.
    stripped = Enum.map(to_flush, fn {arg, _additive, from} -> {arg, from} end)
    event = {:flurry_batch, state.module, state.batch, full_group_tuple, stripped}

    {new_pending, new_group_order} =
      if remaining == [] do
        {Map.delete(state.pending, routing_key), List.delete(state.group_order, routing_key)}
      else
        # Rotate: move this group to the back of the LRU queue.
        rotated = List.delete(state.group_order, routing_key) ++ [routing_key]
        {Map.put(state.pending, routing_key, remaining), rotated}
      end

    {[event], %{state | pending: new_pending, group_order: new_group_order, demand: state.demand - 1}}
  end

  @doc """
  Splits a group tuple into a routing-key tuple and a list of additive values.

  The routing-key tuple contains non-additive values in their original order.
  The additive values list follows the order of `additive_positions`. For
  non-additive batches (`additive_positions == []`), the routing key equals
  the input tuple and the additive values list is empty.
  """
  @spec split_tuple(tuple(), [non_neg_integer()]) :: {tuple(), [term()]}
  def split_tuple(group_tuple, []), do: {group_tuple, []}

  def split_tuple(group_tuple, additive_positions) do
    positions_set = MapSet.new(additive_positions)

    {routing, additive} =
      group_tuple
      |> Tuple.to_list()
      |> Enum.with_index()
      |> Enum.split_with(fn {_v, i} -> not MapSet.member?(positions_set, i) end)

    routing_key = routing |> Enum.map(&elem(&1, 0)) |> List.to_tuple()

    # Preserve the order of `additive_positions` in the returned list,
    # not the order positions happen to appear in the tuple.
    additive_by_position = Map.new(additive, fn {v, i} -> {i, v} end)
    additive_values = Enum.map(additive_positions, &Map.fetch!(additive_by_position, &1))

    {routing_key, additive_values}
  end

  @doc """
  Merges additive values across a list of entries, position by position.

  Each entry's additive values list has the same length as `additive_positions`.
  The default merge concatenates the lists and removes duplicates via
  `Enum.uniq/1`.
  """
  @spec merge_additive_values([{term(), [term()], GenServer.from()}], non_neg_integer()) ::
          [term()]
  def merge_additive_values(_entries, 0), do: []

  def merge_additive_values(entries, num_additive) do
    for i <- 0..(num_additive - 1) do
      entries
      |> Enum.flat_map(fn {_arg, additive_values, _from} ->
        Enum.at(additive_values, i) || []
      end)
      |> Enum.uniq()
    end
  end

  @doc """
  Reconstructs the full group tuple from a routing-key tuple and a list of
  merged additive values by interleaving them back into the original
  positions.
  """
  @spec reconstruct_tuple(tuple(), [term()], [non_neg_integer()]) :: tuple()
  def reconstruct_tuple(routing_key, _merged, []), do: routing_key

  def reconstruct_tuple(routing_key, merged_additives, additive_positions) do
    total_arity = tuple_size(routing_key) + length(additive_positions)
    additive_map = additive_positions |> Enum.zip(merged_additives) |> Map.new()
    routing_list = Tuple.to_list(routing_key)

    {full, _} =
      Enum.reduce(0..(total_arity - 1), {[], routing_list}, fn pos, {acc, r_rem} ->
        case Map.fetch(additive_map, pos) do
          {:ok, value} -> {[value | acc], r_rem}
          :error -> {[hd(r_rem) | acc], tl(r_rem)}
        end
      end)

    full |> Enum.reverse() |> List.to_tuple()
  end
end
