defmodule BotArmySentry.Dispatcher do
  @moduledoc """
  Handles the "Assignment" phase: converting a detected failure into a 
  durable message for the Junior Agent's mailbox.
  """

  @junior_label "hermes_junior"

  def assign_task(trigger, bot, severity \\ "HIGH") do
    payload = %{
      assignment_id: Ecto.UUID.generate(),
      trigger: trigger,
      bot: bot,
      severity: severity,
      timestamp: DateTime.utc_now(),
      instruction:
        "Sentry detected a #{trigger} for #{bot}. Please run the appropriate recovery SOP."
    }

    # We publish to the agent's inbox subject. 
    # The Bot Army infrastructure handles the durable storage in the KV mailbox.
    subject = "agent.session.#{@junior_label}.inbox"

    case publish_assignment(subject, payload) do
      {:ok, _subject} ->
        IO.puts("[Sentry] Assigned #{trigger} for #{bot} to #{@junior_label}")
        :ok

      {:error, reason} ->
        IO.puts("[Sentry] Failed to assign task: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp publish_assignment(subject, payload) do
    # Publisher.publish handles JSON encoding and envelope logic.
    BotArmyLibraryRuntime.NATS.Publisher.publish(subject, payload)
  end
end
