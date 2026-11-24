defmodule BotArmySentry.NATS.Consumer do
  @moduledoc """
  NATS message consumer for sentry.

  Subscribes to NATS subjects and routes messages to handlers.
  Uses standardized Reply format for request/reply patterns.

  All request/reply handlers should return responses using Reply helpers:
  - BotArmyLibraryRuntime.NATS.Reply.ok(data) for success
  - BotArmyLibraryRuntime.NATS.Reply.error(message, code) for errors
  """

  use GenServer
  require Logger

  @reconnect_delay_ms 5000
  @version Mix.Project.config()[:version]
  # Re-register every 20s: renews the Registry entry (40s stale sweep) and
  # re-broadcasts presence so peers that booted before us still see us
  # (single boot-time broadcasts get missed by registries that subscribe
  # after them — the simultaneous-boot race; phase-04 auditor fleet).
  @registry_heartbeat_ms 20_000

  # Register subjects with their metadata for runtime discovery
  @subjects [
    # Add your subjects here:
    # %{subject: "example.task.list", type: :request_reply, description: "List tasks"},
    # %{subject: "example.event.>", type: :subscribe, description: "Example events"}
  ]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    Logger.info("Starting NATS consumer")

    state = %{
      subscriptions: [],
      conn: nil,
      opts: opts
    }

    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    case GenServer.call(BotArmyLibraryRuntime.NATS.Connection, :get_connection, 5000) do
      {:ok, conn} ->
        handle_connection_established(conn, state)

      {:error, _reason} ->
        Logger.warning("NATS connection not ready, will retry")
        Process.send_after(self(), :connect_retry, @reconnect_delay_ms)
        {:noreply, state}
    end
  end

  defp handle_connection_established(conn, state) do
    BotArmyLibraryRuntime.NATS.Connection.subscribe_to_status()
    Logger.info("Connected to NATS, subscribing to topics")

    subscriptions =
      [
        # Add your subjects here
      ]
      |> Enum.map(&subscribe_to_subject(conn, &1))
      |> Enum.filter(&(not is_nil(&1)))

    BotArmyLibraryRuntime.Registry.register("sentry", @subjects, @version)
    Process.send_after(self(), :registry_heartbeat, @registry_heartbeat_ms)

    {:noreply, %{state | subscriptions: subscriptions, conn: conn}}
  end

  defp subscribe_to_subject(conn, subject) do
    case Gnat.sub(conn, self(), subject) do
      {:ok, sub} ->
        Logger.info("Subscribed to #{subject}")
        sub

      {:error, reason} ->
        Logger.error("Failed to subscribe to #{subject}: #{inspect(reason)}")
        nil
    end
  end

  @impl true
  def handle_info(:connect_retry, state) do
    {:noreply, state, {:continue, :connect}}
  end

  @impl true
  def handle_info(:registry_heartbeat, state) do
    # Re-register: renews the local entry past the Registry's 40s stale sweep
    # and re-broadcasts presence to peers. Without this, a fleet that boots
    # simultaneously converges on NOTHING: every registry subscribes after
    # its peers' single broadcast, then the sweep wipes even local entries.
    if state.conn do
      BotArmyLibraryRuntime.Registry.register("sentry", @subjects, @version)
      Process.send_after(self(), :registry_heartbeat, @registry_heartbeat_ms)
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:msg, msg}, state) do
    BotArmyLibraryRuntime.Tracing.with_consumer_span(msg.topic, Map.get(msg, :headers), fn ->
      Logger.debug("Received NATS message on subject: #{msg.topic}")

      if msg.reply_to do
        handle_request_reply(msg, state)
      else
        handle_pub_sub(msg)
      end
    end)

    {:noreply, state}
  end

  defp handle_request_reply(msg, _state) do
    case msg.topic do
      # Add your request/reply handlers here
      # "example.task.list" ->
      #   handle_task_list(msg, _state)
      _ ->
        Logger.debug("Unknown request/reply subject: #{msg.topic}")
    end
  end

  defp handle_pub_sub(msg) do
    case BotArmyLibraryCore.NATS.Decoder.decode(msg.body) do
      {:ok, decoded_message} ->
        route_message(decoded_message, msg.topic)

      {:error, reason} ->
        Logger.warning("Failed to decode message from #{msg.topic}: #{inspect(reason)}")
    end
  end

  @impl true
  def handle_info({:nats, :disconnected}, state) do
    Logger.warning("Disconnected from NATS, will reconnect")
    Process.send_after(self(), :connect_retry, @reconnect_delay_ms)
    {:noreply, %{state | subscriptions: [], conn: nil}}
  end

  @impl true
  def handle_info({:nats, :connected}, state) do
    Logger.info("Reconnected to NATS, re-subscribing")
    {:noreply, state, {:continue, :connect}}
  end

  @impl true
  def handle_info(:reconnect, state) do
    {:noreply, state, {:continue, :connect}}
  end

  # Message routing
  defp route_message(_message, topic) do
    # Route decoded messages to appropriate handlers
    Logger.debug("Routing message from #{topic}")
  end

  # Request/reply handlers
  # defp handle_task_list(msg, state) do
  #   response =
  #     case get_tasks() do
  #       {:ok, tasks} ->
  #         BotArmyLibraryRuntime.NATS.Reply.ok(%{"tasks" => tasks})
  #
  #       {:error, reason} ->
  #         BotArmyLibraryRuntime.NATS.Reply.error(inspect(reason), :list_failed)
  #     end
  #
  #   if state.conn do
  #     Gnat.pub(state.conn, msg.reply_to, response)
  #   end
  # end
end
