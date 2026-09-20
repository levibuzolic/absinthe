defmodule Absinthe.Phase.Document.Validation.IncrementalDirectives do
  @moduledoc false

  use Absinthe.Phase

  alias Absinthe.{Blueprint, Phase, Type}
  alias Absinthe.Blueprint.{Document, Input}
  alias Absinthe.Incremental.Directives

  @spec run(Blueprint.t(), Keyword.t()) :: Phase.result_t()
  def run(input, _options \\ []) do
    if Directives.enabled?(input.schema) do
      validate(input)
    else
      {:ok, input}
    end
  end

  defp validate(input) do
    {input, {_, errors}} = Blueprint.prewalk(input, {%{}, []}, &validate_node/2)
    fragments = Map.new(input.fragments, &{&1.name, &1})

    errors =
      Enum.reduce(input.operations, errors, fn operation, errors ->
        errors =
          if operation.type in [:mutation, :subscription] do
            {errors, _} =
              validate_selections(operation.selections, fragments, MapSet.new(), errors, :root)

            errors
          else
            errors
          end

        if operation.type == :subscription do
          {errors, _} =
            validate_selections(
              operation.selections,
              fragments,
              MapSet.new(),
              errors,
              :subscription
            )

          errors
        else
          errors
        end
      end)

    {:ok, %{input | errors: input.errors ++ Enum.reverse(Enum.uniq(errors))}}
  end

  defp validate_node(%Blueprint.Directive{} = directive, acc) do
    acc = if Directives.identifier(directive), do: validate_label(directive, acc), else: acc
    {directive, acc}
  end

  defp validate_node(%Document.Field{schema_node: %{type: type}} = field, {labels, errors}) do
    errors =
      if list_type?(type) do
        errors
      else
        field.directives
        |> Enum.filter(&(Directives.identifier(&1) == :stream))
        |> Enum.reduce(errors, fn directive, errors ->
          [
            error("Directive `#{directive.name}` may only be used on list fields.", directive)
            | errors
          ]
        end)
      end

    {field, {labels, errors}}
  end

  defp validate_node(node, acc), do: {node, acc}

  defp validate_label(directive, {labels, errors}) do
    case raw_argument(directive, :label) do
      %Input.Variable{} ->
        {labels,
         [
           error("Directive `#{directive.name}` label must be a string literal.", directive)
           | errors
         ]}

      %Input.String{value: label} ->
        case Map.fetch(labels, label) do
          {:ok, previous} ->
            {labels,
             [
               error("Incremental directive label `#{label}` must be unique.", [
                 previous,
                 directive
               ])
               | errors
             ]}

          :error ->
            {Map.put(labels, label, directive), errors}
        end

      _ ->
        {labels, errors}
    end
  end

  defp list_type?(%Type.NonNull{of_type: type}), do: list_type?(type)
  defp list_type?(%Type.List{}), do: true
  defp list_type?(_), do: false

  defp validate_selections(selections, fragments, visited, errors, mode) do
    Enum.reduce(selections, {errors, visited}, fn selection, {errors, visited} ->
      if mode == :subscription and may_be_excluded?(selection) do
        {errors, visited}
      else
        errors = validate_selection(selection, errors, mode)

        case selection do
          %Document.Fragment.Spread{name: name} ->
            if MapSet.member?(visited, name) do
              {errors, visited}
            else
              visited = MapSet.put(visited, name)

              case Map.get(fragments, name) do
                nil ->
                  {errors, visited}

                fragment ->
                  validate_selections(fragment.selections, fragments, visited, errors, mode)
              end
            end

          %Document.Field{} when mode == :root ->
            {errors, visited}

          %{selections: nested} ->
            validate_selections(nested, fragments, visited, errors, mode)
        end
      end
    end)
  end

  defp validate_selection(selection, errors, mode) do
    selection.directives
    |> Enum.filter(&Directives.identifier/1)
    |> Enum.reduce(errors, fn directive, errors ->
      case mode do
        :root ->
          [
            error(
              "Directive `#{directive.name}` is not allowed at a mutation or subscription root.",
              directive
            )
            | errors
          ]

        :subscription ->
          case raw_argument(directive, :if) do
            %Input.Boolean{value: false} ->
              errors

            %Input.Variable{} ->
              errors

            _ ->
              [
                error(
                  "Directive `#{directive.name}` must be disableable in a subscription operation.",
                  directive
                )
                | errors
              ]
          end
      end
    end)
  end

  # A variable can exclude this selection at execution time. This validation is
  # deliberately independent of the variables supplied for the selected operation.
  defp may_be_excluded?(selection) do
    Enum.any?(selection.directives, fn directive ->
      case {directive.schema_node, raw_argument(directive, :if)} do
        {%{identifier: :skip}, %Input.Boolean{value: false}} -> false
        {%{identifier: :skip}, _} -> true
        {%{identifier: :include}, %Input.Boolean{value: true}} -> false
        {%{identifier: :include}, _} -> true
        _ -> false
      end
    end)
  end

  defp raw_argument(directive, name) do
    case Enum.find(directive.arguments, fn
           %{schema_node: %{identifier: ^name}} -> true
           _ -> false
         end) do
      %{input_value: %{raw: %{content: value}}} -> value
      _ -> nil
    end
  end

  defp error(message, nodes) do
    %Phase.Error{
      phase: __MODULE__,
      message: message,
      locations: Enum.map(List.wrap(nodes), & &1.source_location)
    }
  end
end
