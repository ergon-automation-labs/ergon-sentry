defmodule BotArmySentry.Application do
  @moduledoc """
  Sentry Bot application supervisor.

  Follows bot army pattern with environment-aware startup:
  - PulsePublisher sends `system.health` liveness every 30s and rich `bot.<service>.pulse` every 30 minutes
  - Workers not started in :test (gated by env check)

  Observability: see `PulsePublisher` — fleet UIs keyed on Synapse hydration should use `system.health` freshness (90s), not pulse interval alone.
  """

  use Application

  @impl true
  def start(_type, _args) do
    # Note: BotArmyLibraryRuntime.Telemetry and BotArmyLibraryRuntime.NATS.Connection are started
    # by bot_army_runtime automatically — do not add them here.

    children =
      []
      |> maybe_add_pulse_publisher()
      |> maybe_add_workers()

    opts = [strategy: :one_for_one, name: BotArmySentry.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp maybe_add_pulse_publisher(children) do
    if Application.get_env(:bot_army_sentry, :env) == :test do
      children
    else
      [{BotArmySentry.PulsePublisher, []} | children]
    end
  end

  defp maybe_add_workers(children) do
    if Application.get_env(:bot_army_sentry, :env) == :test do
      children
    else
      # Bot-specific workers and pollers go here (GenServers that do async work)
      # Examples: Scheduler, Poller, Watcher
      # Pattern: gated with if Application.get_env(:bot_army_sentry, :env) == :test to prevent long-running processes in test
      [{BotArmySentry.Monitor, []} | children]
    end
  end
end
