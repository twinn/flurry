defmodule Flurry.Producer do
  @moduledoc false
  # A GenStage producer that buffers individual requests and emits them as
  # a single batch event per flush. Flush policy:
  #
  #   * any group's pending >= batch_size → flush that group
  #   * mailbox empty → flush the least-recently-used non-empty group
  #   * otherwise → hold, we expect more requests
  #
  # The "mailbox empty" check uses `Process.info(self(), :message_queue_len)`
  # and runs inside GenStage callbacks, i.e. at the moment we are about to
  # return from `handle_call` / `handle_demand`. A zero length at that moment
  # means "no further requests are immediately queued for us" — it's safe to
  # flush now instead of holding.
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
      pending: %{},
      group_order: [],
      priority: [],
      demand: 0
    }

    {:producer, state}
  end

  @impl true
  def handle_call({:enqueue, group_key, arg}, from, state) do
    state = enqueue(state, group_key, arg, from)
    {events, state} = do_maybe_emit(state)
    # We intentionally don't reply to `from` here — the consumer will reply
    # once the batch runs. `:noreply` leaves the caller blocked.
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

  defp enqueue(state, group_key, arg, from) do
    existing = Map.get(state.pending, group_key, [])
    new_list = existing ++ [{arg, from}]
    new_pending = Map.put(state.pending, group_key, new_list)

    new_group_order =
      if Map.has_key?(state.pending, group_key),
        do: state.group_order,
        else: state.group_order ++ [group_key]

    %{state | pending: new_pending, group_order: new_group_order}
  end

  defp do_maybe_emit(state) do
    {:message_queue_len, qlen} = Process.info(self(), :message_queue_len)
    maybe_emit(state, qlen)
  end

  @doc """
  Pure decision function for the producer's flush policy. Extracted so the
  mailbox-peek behavior can be exercised deterministically in unit tests
  without coordinating real mailbox state.

  Priority batches (pre-formed by the consumer via bisect) take precedence
  over pending — we drain the entire priority queue one batch at a time
  before emitting anything from pending. Priority items carry their own
  group key so they route to the correct group's consumer context.

  Pending emission picks a group by:
    1. Scanning for any group whose pending count has reached `batch_size`.
    2. Otherwise, if the mailbox is empty, picking the front (LRU) of
       `group_order`.
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

    cond do
      saturated != nil -> flush_group(state, saturated)
      qlen == 0 -> flush_group(state, hd(state.group_order))
      true -> {[], state}
    end
  end

  defp find_saturated(state) do
    Enum.find(state.group_order, fn gk ->
      length(Map.get(state.pending, gk, [])) >= state.batch_size
    end)
  end

  defp flush_group(state, group_key) do
    entries = Map.get(state.pending, group_key, [])
    {to_flush, remaining} = Enum.split(entries, state.batch_size)

    event = {:flurry_batch, state.module, state.batch, group_key, to_flush}

    {new_pending, new_group_order} =
      if remaining == [] do
        {Map.delete(state.pending, group_key), List.delete(state.group_order, group_key)}
      else
        # Rotate: move this group to the back of the LRU queue.
        rotated = List.delete(state.group_order, group_key) ++ [group_key]
        {Map.put(state.pending, group_key, remaining), rotated}
      end

    {[event], %{state | pending: new_pending, group_order: new_group_order, demand: state.demand - 1}}
  end
end
