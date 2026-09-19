defmodule IncrementalHTTP.MixProject do
  use Mix.Project

  def project do
    [
      app: :incremental_http,
      version: "0.0.0",
      elixir: "~> 1.17",
      deps: [{:absinthe, path: "../.."}, {:jason, "1.4.4"}]
    ]
  end

  def application, do: [extra_applications: [:logger]]
end
