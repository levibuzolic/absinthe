defmodule Absinthe.Incremental.Directives do
  @moduledoc false

  alias Absinthe.{Blueprint, Schema, Type}

  @type directive_name :: :defer | :stream

  @spec enabled?(Schema.t()) :: boolean
  def enabled?(schema) do
    Enum.any?([:defer, :stream], fn name ->
      not is_nil(identifier(Schema.lookup_directive(schema, name)))
    end)
  end

  @spec identifier(Blueprint.Directive.t() | Type.Directive.t() | nil) :: directive_name | nil
  def identifier(%Blueprint.Directive{schema_node: directive}), do: identifier(directive)

  def identifier(%Type.Directive{
        identifier: identifier,
        definition: Type.BuiltIns.IncrementalDirectives
      })
      when identifier in [:defer, :stream] do
    identifier
  end

  def identifier(_), do: nil

  @spec active(%{optional(atom) => term, directives: [Blueprint.Directive.t()]}, directive_name) ::
          {Blueprint.Directive.t(), map()} | nil
  def active(%{directives: directives}, identifier) do
    Enum.find_value(directives, fn directive ->
      if identifier(directive) == identifier do
        args = Blueprint.Input.Argument.value_map(directive.arguments)
        if args.if, do: {directive, args}
      end
    end)
  end
end
