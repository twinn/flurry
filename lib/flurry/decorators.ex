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

  defp register({singular, _meta, [first_arg | rest_args]}, opts, context) when is_atom(singular) do
    key = arg_name!(first_arg, singular)
    group_args = Enum.map(rest_args, &arg_name!(&1, singular))

    returns = Keyword.get(opts, :returns, :one)
    batch_size = Keyword.get(opts, :batch_size)
    on_failure = Keyword.get(opts, :on_failure, :bisect)
    # `in_transaction` default is resolved at __before_compile__ time,
    # because the default depends on the module-level :repo option which
    # the decorator can't see. Store nil here and let before_compile fill
    # it in based on the repo.
    in_transaction = Keyword.get(opts, :in_transaction)

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

    if in_transaction != nil and in_transaction not in [:warn, :safe, :bypass] do
      raise ArgumentError,
            "Flurry: invalid `:in_transaction` option #{inspect(in_transaction)} — must be :warn, :safe, or :bypass"
    end

    Module.put_attribute(context.module, :flurry_batches, %{
      singular: singular,
      key: key,
      group_args: group_args,
      bulk: context.name,
      arity: context.arity,
      returns: returns,
      batch_size: batch_size,
      on_failure: on_failure,
      in_transaction: in_transaction
    })
  end

  defp register(other, _opts, _context) do
    raise ArgumentError, """
    Flurry: malformed @decorate batch(...) call signature.

    Expected: `batch(singular_name(arg_name))` for single-arg batching, or
              `batch(singular_name(arg_name, group_arg_1, group_arg_2))` for
              group-keyed batching. The first argument is the batched
              variable; remaining arguments form the group key.

    Got: #{Macro.to_string(other)}
    """
  end

  defp arg_name!({name, _meta, ctx}, _singular) when is_atom(name) and is_atom(ctx), do: name

  defp arg_name!(other, singular) do
    raise ArgumentError,
          "Flurry: malformed argument in @decorate batch(#{singular}(...)) — " <>
            "expected a bare name, got: #{Macro.to_string(other)}"
  end
end
