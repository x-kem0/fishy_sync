defmodule FishySync.MixProject do
  use Mix.Project

  def project do
    [
      app: :fishy_sync,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {FishySync, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.4"},
      {:postgrex, "~> 0.21.1"},
      {:httpoison, "~> 2.2"}
    ]
  end
end
