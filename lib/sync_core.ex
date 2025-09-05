defmodule SyncCore do
  use GenServer

  def start_link(_opt) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def force_sync(user_id) do
    Process.send(__MODULE__, {:notification, nil, nil, nil, %{
      "followeeId" => user_id,
      "followeeHost" => ":)"
    } |> Jason.encode!()}, [])
  end

  def report_finish(user_id, result) do
    GenServer.call(__MODULE__, {:report_finish, user_id, result})
  end

  def init(_opt) do
    :logger.debug("starting sync core...")

    Process.flag(:trap_exit, true)
    {:ok, synchronizer_pid} = Synchronizer.start_link(nil)

    # initialize database trigger for notifications
    Database.initialize()

    # listen to notifications from db
    {:ok, listener_ref} = Postgrex.Notifications.listen(FishySync.Notifications, "fishysync_notice")

    state = %{
      queue: [],
      current_user: nil,
      synchronizer_pid: synchronizer_pid,
      listener: listener_ref,
      synchronizing_role_id: Database.get_role_id_by_name("Synchronizing"),
      synchronized_role_id: Database.get_role_id_by_name("Synchronized"),
      synchronization_failed_role_id: Database.get_role_id_by_name("Synchronization Failed"),
      home_host: System.fetch_env!("HOME_HOST"),
      bearer_token: System.fetch_env!("BEARER_TOKEN")
    }

    :logger.info("sync core ready")
    {:ok, state}
  end

  # synchronizer crashed, restart
  def handle_info({:EXIT, _pid, _reason}, state) do
    :logger.error("synchronizer crash")
    if state.current_user != nil do
      unassign_role(state.current_user, state.synchronizing_role_id, state)
      assign_role(state.current_user, state.synchronization_failed_role_id, state)
    end
    {:ok, synchronizer_pid} = Synchronizer.start_link(nil)
    state = 
      %{state | synchronizer_pid: synchronizer_pid, current_user: nil}
      |> sync()
    {:noreply, state}
  end

  # notification of a new follow from the database
  def handle_info({:notification, _notification_pid, _listen_ref, _channel, message}, state) do
    result =
      with {:ok, json} <- Jason.decode(message),
           :ok <- followee_remote_check(json),
           {:ok, user_id} <- get_user_id(json),
           :ok <- user_already_queued_check(user_id, state),
           :ok <- user_already_synced_check(user_id, state) do
        {:ok, user_id}
      end

    state = 
      case result do
        {:disregard, reason} -> 
          :logger.debug("disregarding: #{inspect reason}")
          state
        {:error, err} -> 
          :logger.error("error syncing user: #{inspect err}")
          state
        {:ok, user_id} -> 
          :logger.debug("queuing user #{user_id}")
          queue = [user_id | state.queue]
          %{state| queue: queue}
      end

    state = sync(state)

    {:noreply, state}
  end

  def handle_call({:report_finish, user_id, result}, _from, state) do
    unassign_role(user_id, state.synchronizing_role_id, state)
    case result do
      :success ->
        assign_role(user_id, state.synchronized_role_id, state)
      :failure ->
        assign_role(user_id, state.synchronization_failed_role_id, state)
    end
    if user_id != state.current_user do
      :logger.error("Synchronizer reported different user id than expected")
      {:reply, :ok, state}
    else
      :logger.info("synchronization finished for user id #{user_id}")
      state = 
        %{state |
          current_user: nil
        } |> sync()
      {:reply, :ok, state}
    end
  end

  defp followee_remote_check(json) do
    case json do
      %{"followeeHost" => nil} -> {:disregard, :local_user}
      %{"followeeHost" => _} -> :ok
      _ -> {:error, :invalid_json}
    end
  end

  defp get_user_id(message) do
    case message do
      %{"followeeId" => user_id} -> {:ok, user_id}
      _ -> {:error, :invalid_json}
    end
  end

  defp user_already_queued_check(user, state) do
    if Enum.member?(state.queue, user) || state.current_user == user do
      {:disregard, :user_already_queued}
    else
      :ok
    end
  end

  defp user_already_synced_check(user_id, state) do
    if Database.user_is_member_of?(user_id, state.synchronized_role_id) do
      {:disregard, :user_already_synced}
    else
      :ok
    end
  end

  defp sync(state) do
    if state.current_user == nil do

      case state.queue do
        [user_id | tail] ->
          assign_role(user_id, state.synchronizing_role_id, state)
          Synchronizer.start_sync(state.synchronizer_pid, user_id)
          %{state |
            current_user: user_id,
            queue: tail
          }
        [] -> state
      end

    else
      state
    end
  end

  defp unassign_role(user_id, role_id, state) do
    HTTPoison.post(
      "http://localhost:3000/api/admin/roles/unassign",
      %{
        "roleId" => role_id,
        "userId" => user_id
      } |> Jason.encode!(),
      [
        {"Content-Type", "application/json"},
        {"Authorization", "Bearer #{state.bearer_token}"}
      ]
    )
  end

  defp assign_role(user_id, role_id, state) do
    HTTPoison.post(
      "http://localhost:3000/api/admin/roles/assign",
      %{
        "roleId" => role_id,
        "userId" => user_id
      } |> Jason.encode!(),
      [
        {"Content-Type", "application/json"},
        {"Authorization", "Bearer #{state.bearer_token}"}
      ]
    )
  end

end
