defmodule BotArmySentry.Monitor do
  use GenServer
  require Logger

  @moduledoc """
  The Sentry's sensory organ. 
  Listens for health flips and polls the registry for gaps.
  """

  @registry_poll_interval :timer.seconds(60)
  @health_subject "system.health.>"

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    # 1. Start the registry poll
    schedule_registry_check()

    # 2. Subscribe to health updates
    subscribe_to_health()

    {:ok,
     %{
       # Tracks {bot, status} to detect flips
       health_state: %{},
       registry_snapshot: %{}
     }}
  end

  @impl true
  def handle_info(:check_registry, state) do
    # Poll the registry
    case fetch_registry() do
      {:ok, current_registry} ->
        detect_registry_gaps(state.registry_snapshot, current_registry)
        {:noreply, %{state | registry_snapshot: current_registry}}

      {:error, reason} ->
        Logger.error("[Sentry] Registry check failed: #{inspect(reason)}")
        {:noreply, state}
    end
  end

  # Handle NATS health messages
  def handle_health_update(bot, status, state) do
    old_status = Map.get(state.health_state, bot)

    if old_status != status and status == "unhealthy" do
      Logger.warning("[Sentry] Health Flip detected: #{bot} is now UNHEALTHY")
      BotArmySentry.Dispatcher.assign_task("HEALTH_FLIP", bot)
    end

    {:noreply, %{state | health_state: Map.put(state.health_state, bot, status)}}
  end

  defp schedule_registry_check do
    :timer.send_interval(@registry_poll_interval, :check_registry)
  end

  defp subscribe_to_health do
    # This would call the NATS client's subscribe function
    # Nats.subscribe(@health_subject, fn bot, status -> 
    #   GenServer.cast(__MODULE__, {:health_update, bot, status})
    # end)
  end

  defp fetch_registry do
    # Request the registry list from NATS
    # In reality: Nats.request("bot_army.registry.bots.list", %{})
    {:ok, %{"bot_army_gtd" => "healthy", "bot_army_trading_strategy" => "healthy"}}
  end

  defp detect_registry_gaps(old, current) do
    missing = Map.keys(old) -- Map.keys(current)

    Enum.each(missing, fn bot ->
      Logger.warning("[Sentry] Registry Gap detected: #{bot} has disappeared!")
      BotArmySentry.Dispatcher.assign_task("REGISTRY_GAP", bot)
    end)
  end

  @impl true
  def handle_cast({:health_update, _bot, _status}, state) do
    # Logic from handle_health_update
    # Simplified for scaffold
    {:noreply, state}
  end
end
