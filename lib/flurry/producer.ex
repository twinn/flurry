defmodule Flurry.Producer do
  @moduledoc false
  # A GenStage producer that buffers individual requests and emits them as
  # a single batch event per flush. Flush policy:
  #
  #   * pending >= batch_size → flush (capped at batch_size, remainder kept)
  #   * mailbox empty → flush whatever is pending
  #   * otherwise → hold, we expect more requests
  #
  # The "mailbox empty" check uses `Process.info(self(), :message_queue_len)`
  # and runs inside GenStage callbacks, i.e. at the moment we are about to
  # return from `handle_call` / `handle_demand`. A zero length at that moment
  # means "no further requests are immediately queued for us" — it's safe to
  # flush now instead of holding.

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
      pending: [],
      priority: [],
      demand: 0
    }

    {:producer, state}
  end

  @impl true
  def handle_call({:enqueue, arg}, from, state) do
    state = %{state | pending: state.pending ++ [{arg, from}]}
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
  def handle_cast({:requeue_batches, batches}, state) do
    # Append to the end — FIFO across multiple bisect rounds. Each element
    # of `batches` is a list of `{arg, from}` entries; each becomes its own
    # priority slot and will be emitted as its own single event.
    state = %{state | priority: state.priority ++ batches}
    {events, state} = do_maybe_emit(state)
    {:noreply, events, state}
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
  before emitting anything from pending. This keeps bisected halves from
  being re-coalesced into the same failing batch.
  """
  @spec maybe_emit(map(), non_neg_integer()) :: {list(), map()}
  def maybe_emit(%{demand: 0} = state, _qlen), do: {[], state}

  # Priority queue wins — emit first priority batch as-is, untouched.
  def maybe_emit(%{priority: [batch | rest]} = state, _qlen) do
    event = {:flurry_batch, state.module, state.batch, batch}
    {[event], %{state | priority: rest, demand: state.demand - 1}}
  end

  def maybe_emit(%{pending: []} = state, _qlen), do: {[], state}

  def maybe_emit(state, qlen) do
    pending_count = length(state.pending)

    if pending_count >= state.batch_size or qlen == 0 do
      {to_flush, remaining} = Enum.split(state.pending, state.batch_size)
      event = {:flurry_batch, state.module, state.batch, to_flush}
      {[event], %{state | pending: remaining, demand: state.demand - 1}}
    else
      {[], state}
    end
  end
end
