defmodule Absinthe.Type.BuiltIns.IncrementalDirectives do
  @moduledoc """
  Opt-in support for the draft GraphQL incremental delivery directives.

  Import this module into a schema with `import_directives` to make `@defer`
  and `@stream` available. The definitions follow GraphQL spec PR #1110 at
  commit `045e19363c2b55f127960bd3b5e8072a15b29aec`.

  Ordinary execution remains eager by default; importing these directives only
  makes their notation available. The incremental execution API is responsible
  for interpreting their coerced arguments.
  """

  use Absinthe.Schema.Notation

  directive :defer do
    repeatable false

    arg :if, non_null(:boolean), default_value: true
    arg :label, :string

    on [:fragment_spread, :inline_fragment]
  end

  directive :stream do
    repeatable false

    arg :if, non_null(:boolean), default_value: true
    arg :label, :string
    arg :initial_count, non_null(:integer), default_value: 0

    on [:field]
  end
end
