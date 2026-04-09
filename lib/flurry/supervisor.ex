defmodule Flurry.Supervisor do
  @moduledoc false
  # Per-user-module supervisor. Started by the generated `start_link/1` on a
  # module that `use Flurry`s. One producer/consumer pair per decorated
  # function, all restarted together (`one_for_all`) because they are tightly
  # coupled — a crashed consumer orphans a producer's captured callers.

  use Supervisor

  @spec start_link(module(), [map()], keyword()) :: Supervisor.on_start()
  def start_link(module, batches, opts) do
    name = Module.concat(module, Supervisor)
    Supervisor.start_link(__MODULE__, {module, batches, opts}, name: name)
  end

  @impl true
  def init({module, batches, opts}) do
    default_batch_size = Keyword.get(opts, :batch_size, 100)

    children =
      Enum.flat_map(batches, fn batch ->
        producer_name = Flurry.Runtime.producer_name(module, batch.singular)
        consumer_name = Flurry.Runtime.consumer_name(module, batch.singular)
        # A decorator-level `batch_size:` wins over the start_link default.
        effective_batch_size = batch[:batch_size] || default_batch_size

        [
          Supervisor.child_spec(
            {Flurry.Producer, [name: producer_name, module: module, batch: batch, batch_size: effective_batch_size]},
            id: producer_name
          ),
          Supervisor.child_spec(
            {Flurry.Consumer, [name: consumer_name, module: module, batch: batch, producer: producer_name]},
            id: consumer_name
          )
        ]
      end)

    Supervisor.init(children, strategy: :one_for_all)
  end
end
