defmodule PulsarClientErl.MixProject do
  use Mix.Project

  def project() do
    [
      app: :pulsar,
      version: read_version(),
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {:pulsar_app, []}
    ]
  end

  defp deps do
    [
      {:crc32cer, github: "emqx/crc32cer", tag: "0.1.9"},
      {:murmerl3, github: "bipthelin/murmerl3", ref: "5ae165c034e601b52e9569187b33ac95f6b3878e"},
      {:snappyer, github: "emqx/snappyer", tag: "1.2.5"},
      {:replayq, github: "emqx/replayq", tag: "0.3.4"},
      {:redbug, "~> 2.0"}
    ]
  end

  defp read_version() do
    try do
      {out, 0} = System.cmd("git", ["describe", "--tags"])
      out
      |> String.trim()
      |> Version.parse!()
      |> Map.put(:pre, [])
      |> to_string()
    catch
      _ -> "0.1.0"
    end
  end
end
