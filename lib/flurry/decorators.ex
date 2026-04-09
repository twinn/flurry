defmodule Flurry.Decorators do
  @moduledoc """
  Defines the `@decorate batch(...)` decorator used by modules that
  `use Flurry`. You don't normally interact with this module directly —
  `use Flurry` brings the decorator into scope.

  ## Shapes

      @decorate batch(get(id))
      def get_many(ids), do: ...

      @decorate batch(get_by_group(group), returns: :list)
      def get_many_by_group(groups), do: ...

  The first argument is a fake call expression whose *function name* becomes
  the generated singular entry-point and whose *single argument name* becomes
  both the parameter of that entry point and the record field used to
  correlate bulk results back to individual callers.
  """

  use Decorator.Define, batch: 1, batch: 2

  def batch(call_sig, body, context) do
    register(call_sig, [], context)
    body
  end

  def batch(call_sig, opts, body, context) when is_list(opts) do
    register(call_sig, opts, context)
    body
  end

  defp register({singular, _meta, [{key, _key_meta, ctx}]}, opts, context)
       when is_atom(singular) and is_atom(key) and is_atom(ctx) do
    returns = Keyword.get(opts, :returns, :one)
    batch_size = Keyword.get(opts, :batch_size)
    on_failure = Keyword.get(opts, :on_failure, :bisect)

    if returns not in [:one, :list] do
      raise ArgumentError,
            "Flurry: invalid `:returns` option #{inspect(returns)} — must be :one or :list"
    end

    if batch_size != nil and not (is_integer(batch_size) and batch_size > 0) do
      raise ArgumentError,
            "Flurry: invalid `:batch_size` option #{inspect(batch_size)} — must be a positive integer"
    end

    if on_failure not in [:fail_all, :bisect] do
      raise ArgumentError,
            "Flurry: invalid `:on_failure` option #{inspect(on_failure)} — must be :fail_all or :bisect"
    end

    Module.put_attribute(context.module, :flurry_batches, %{
      singular: singular,
      key: key,
      bulk: context.name,
      arity: context.arity,
      returns: returns,
      batch_size: batch_size,
      on_failure: on_failure
    })
  end

  defp register(other, _opts, _context) do
    raise ArgumentError, """
    Flurry: malformed @decorate batch(...) call signature.

    Expected: `batch(singular_name(arg_name))` or
              `batch(singular_name(arg_name), returns: :one | :list)`

    Got: #{Macro.to_string(other)}
    """
  end
end
