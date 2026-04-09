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
    # `correlate` defaults to the first arg's name, i.e. the same as
    # `key`. Users override when the record's field name differs from
    # the caller's arg name.
    correlate = Keyword.get(opts, :correlate, key)
    timeout = Keyword.get(opts, :timeout, 5_000)
    # `batch_by` arrives as AST because it's a function expression
    # (closure or capture) — it doesn't get evaluated by the decorator
    # library, only stashed for `__before_compile__` to unquote into
    # the generated entry point.
    batch_by = Keyword.get(opts, :batch_by)

    # Data-driven validation: `for` walks the list once and raises on the
    # first invalid entry. Adding a new option is a one-liner here, and
    # the cyclomatic complexity of `register/3` stays flat regardless of
    # how many options we add.
    validators = [
      {:returns, returns, &(&1 in [:one, :list]), "must be :one or :list"},
      {:batch_size, batch_size, &(&1 == nil or (is_integer(&1) and &1 > 0)), "must be a positive integer"},
      {:on_failure, on_failure, &(&1 in [:fail_all, :bisect]), "must be :fail_all or :bisect"},
      {:in_transaction, in_transaction, &(&1 == nil or &1 in [:warn, :safe, :bypass]),
       "must be :warn, :safe, or :bypass"},
      {:correlate, correlate, &Flurry.Decorators.valid_correlate?/1,
       "must be an atom (record field name) or a function expression like `fn r -> r.nested.id end`"},
      {:timeout, timeout, &(is_integer(&1) and &1 > 0), "must be a positive integer (milliseconds)"},
      {:batch_by, batch_by, &(&1 == nil or Flurry.Decorators.function_ast?(&1)),
       "must be a function expression (e.g. `fn tuple -> ... end` or `&MyMod.fun/1`)"}
    ]

    for {name, value, valid?, desc} <- validators, not valid?.(value) do
      raise ArgumentError,
            "Flurry: invalid `#{inspect(name)}` option #{inspect(value)} — #{desc}"
    end

    if batch_by != nil and group_args == [] do
      raise ArgumentError,
            "Flurry: `:batch_by` on a single-arg decoration (batch(#{singular}(#{key}))) " <>
              "is meaningless — single-arg decorations have nothing to normalize beyond " <>
              "the batched argument itself. Either remove :batch_by, or add additional " <>
              "arguments to the decorator signature."
    end

    Module.put_attribute(context.module, :flurry_batches, %{
      singular: singular,
      key: key,
      correlate: correlate,
      group_args: group_args,
      bulk: context.name,
      arity: context.arity,
      returns: returns,
      batch_size: batch_size,
      on_failure: on_failure,
      in_transaction: in_transaction,
      timeout: timeout,
      # Stored as raw AST — `__before_compile__` unquotes it directly
      # into the generated entry point, then strips this key from the
      # batch map before it gets escaped into `__flurry_batches__/0`.
      batch_by: batch_by
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

  @doc false
  # Public only so the validator closures in `register/3` can call it
  # (they can't reference module-local private functions from inside a
  # closure captured in a local variable).
  def function_ast?({:fn, _, _}), do: true
  def function_ast?({:&, _, _}), do: true
  def function_ast?(_), do: false

  @doc false
  def valid_correlate?(c) when is_atom(c), do: true
  def valid_correlate?(c), do: function_ast?(c)

  defp arg_name!({name, _meta, ctx}, _singular) when is_atom(name) and is_atom(ctx), do: name

  defp arg_name!(other, singular) do
    raise ArgumentError,
          "Flurry: malformed argument in @decorate batch(#{singular}(...)) — " <>
            "expected a bare name, got: #{Macro.to_string(other)}"
  end
end
