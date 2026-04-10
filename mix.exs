defmodule Flurry.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/twinn/flurry"

  def project do
    [
      app: :flurry,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      description: description(),
      package: package(),
      docs: docs(),
      dialyzer: dialyzer(),
      source_url: @source_url
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:gen_stage, "~> 1.2"},
      {:decorator, "~> 1.4"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:styler, "~> 1.4", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp dialyzer do
    [
      plt_file: {:no_warnings_as_errors, "priv/plts/project.plt"},
      plt_core_path: "priv/plts/core.plt",
      flags: [:error_handling, :unknown, :unmatched_returns]
    ]
  end

  defp description do
    "Scatter-gather batching for Elixir built on GenStage. " <>
      "Individual requests are coalesced into a single bulk call and " <>
      "results are correlated back to each caller. Uses mailbox-peek " <>
      "flushing instead of timers."
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "Flurry",
      extras: ["README.md", "CHANGELOG.md", "LICENSE"]
    ]
  end
end
