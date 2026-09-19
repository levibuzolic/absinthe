defmodule Absinthe.Utils.Render do
  @moduledoc false

  import Inspect.Algebra

  def multiline(docs, true) do
    force_unfit(docs)
  end

  def multiline(docs, false) do
    docs
  end

  def join(docs, joiner) do
    fold_doc(docs, fn doc, acc ->
      concat([doc, concat(List.wrap(joiner)), acc])
    end)
  end

  def render_string_value(string, indent \\ 2) do
    string
    |> String.trim()
    |> String.split("\n")
    |> case do
      [string_line] ->
        render_quoted_string(string_line)

      string_lines ->
        concat(
          nest(
            block_string([~s(""")] ++ string_lines),
            indent,
            :always
          ),
          concat(line(), ~s("""))
        )
    end
  end

  def render_quoted_string(string) do
    ~s(") <> escape_string(string) <> ~s(")
  end

  @escaped_chars [?", ?\\, ?/, ?\b, ?\f, ?\n, ?\r, ?\t]

  defp escape_string(string) do
    for char <- String.to_charlist(string), into: "" do
      cond do
        char in @escaped_chars -> to_string(escape_char(char))
        char < 0x20 -> "\\u" <> String.pad_leading(Integer.to_string(char, 16), 4, "0")
        true -> <<char::utf8>>
      end
    end
  end

  defp escape_char(?"), do: [?\\, ?"]
  defp escape_char(?\\), do: [?\\, ?\\]
  defp escape_char(?/), do: [?\\, ?/]
  defp escape_char(?\b), do: [?\\, ?b]
  defp escape_char(?\f), do: [?\\, ?f]
  defp escape_char(?\n), do: [?\\, ?n]
  defp escape_char(?\r), do: [?\\, ?r]
  defp escape_char(?\t), do: [?\\, ?t]

  defp block_string([string]) do
    string(string)
  end

  defp block_string([string | rest]) do
    string
    |> string()
    |> concat(block_string_line(rest))
    |> concat(block_string(rest))
  end

  defp block_string_line(["", _ | _]), do: nest(line(), :reset)
  defp block_string_line(_), do: line()
end
