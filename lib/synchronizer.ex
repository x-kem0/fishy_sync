defmodule Synchronizer do
  use GenServer

  @worker_count 3
  @headers [{"Content-Type", "application/json"}]

  def report_worker_finish(pid) do
    GenServer.call(pid, :worker_finished)
  end

  def start_sync(pid, user_id) do
    GenServer.call(pid, {:start_sync, user_id})
  end

  def start_link(opt) do
    GenServer.start_link(__MODULE__, opt)
  end

  def init(_opt) do
    # start @worker_count workers and record their state as idle
    workers =
      for _ <- 1..@worker_count do
        SyncWorker.start_link(self())
      end
      |> Enum.reduce(%{}, fn {:ok, worker_pid}, acc ->
        Map.put(acc, worker_pid, :idle)
      end)

    state = %{
      state: :idle,
      current_user: nil,
      workers: workers
    }

    {:ok, state}
  end

  def handle_call({:start_sync, user_id}, _from, %{state: :idle} = state) do
    {:reply, :ok, %{state | state: :busy}, {:continue, user_id}}
  end

  def handle_call(:worker_finished, {worker_pid, _}, state) do
    workers = Map.put(state.workers, worker_pid, :idle)

    case workers |> Map.values() |> Enum.find(&(&1 == :busy)) do
      nil ->
        # no workers are busy, we're done!
        SyncCore.report_finish(state.current_user, :success)
        {:reply, :ok, %{state | state: :idle, current_user: nil, workers: workers}}
      _ ->
        {:reply, :ok, %{state | workers: workers}}
    end
  end

  defp distribute_chunks([chunk | chunk_tail], [worker_pid | worker_tail]) do
    SyncWorker.start_chunk(worker_pid, chunk)
    distribute_chunks(chunk_tail, worker_tail)
  end

  defp distribute_chunks([], []) do
    :ok
  end

  def handle_continue(user_db_id, state) do
    # get username
    {username, host} = Database.get_username(user_db_id)
    fully_qualified_username = "#{username}@#{host}"
    :logger.info("synchronzing user #{fully_qualified_username}")

    result = with {:ok, url} <- get_url(host),
      {:ok, instance_type} = check_instance_type(url),
      {:ok, user_id} <- get_user_id(url, instance_type, username, host),
      {:ok, posts} <- get_posts(url, instance_type, user_id),
      notes <- post_id_to_note_url(posts, url, instance_type, username) do
        :logger.info("user #{fully_qualified_username} | type: #{instance_type} | id: #{user_id} | #{length(posts)} posts")
        {:ok, notes}
    end

    case result do
      {:ok, []} ->
        # no notes, no sync
        SyncCore.report_finish(user_db_id, :success)
        {:noreply, %{state | state: :idle, current_user: nil}}

      {:ok, notes} ->
        chunk_size = Float.ceil(length(notes)/@worker_count) |> trunc()
        chunks = notes |> Enum.chunk_every(chunk_size)
        distribute_chunks(chunks, Map.keys(state.workers))
        workers = Map.keys(state.workers)
          |> Enum.reduce(%{}, fn worker_pid, acc ->
            Map.put(acc, worker_pid, :busy)
          end)
        {:noreply, %{state | current_user: user_db_id, workers: workers}}

      err ->
        :logger.error("synchronizer failed #{inspect err}")
        SyncCore.report_finish(user_db_id, :failure)
        {:noreply, %{state | state: :idle}}
    end

  end

  defp get_url(host) do
    url = "https://#{host}"
    uri = URI.parse(url)
    if uri.scheme != nil && uri.host =~ "." do
      {:ok, url}
    else
      {:error, :invalid_host}
    end
  end

  defp check_instance_type(url) do
    {:ok, %{status_code: mastodon_status_code}} = HTTPoison.get("#{url}/api/v2/instance", @headers)
    {:ok, %{status_code: misskey_status_code}} = HTTPoison.post("#{url}/api/meta", "{\"detail\": false}", @headers)
    case {misskey_status_code, mastodon_status_code} do
      {200, _} -> {:ok, :misskey}
      {_, 200} -> {:ok, :mastodon}
      _ -> {:error, :incompatible_instance}
    end
  end

  defp get_user_id(url, :misskey, username, host) do
    result = HTTPoison.post(
      "#{url}/api/users/search-by-username-and-host",
      %{
        username: username,
        host: host,
        detail: false
      } |> Jason.encode!(),
      @headers
    )
    
    case result do
      {:ok, %{status_code: 200, body: body}} ->
        case body |> Jason.decode!() |> Enum.find(&(Map.get(&1, "username") == username)) do
          %{"id" => user_id} -> {:ok, user_id}
          _ -> {:error, :could_not_find_user}
        end
      _ ->
        {:error, :invalid_user_id_response}
    end

  end

  defp get_user_id(url, :mastodon, username, host) do
    result = HTTPoison.get(
      "#{url}/api/v1/accounts/lookup?acct=#{username}@#{host}",
      @headers
    )
    
    case result do
      {:ok, %{status_code: 200, body: body}} ->
        case body |> Jason.decode!() do
          %{"id" => user_id} -> {:ok, user_id}
          _ -> {:error, :could_not_find_user}
        end
      _ ->
        {:error, :invalid_user_id_response}
    end

  end

  defp get_posts(url, :misskey, user_id) do
    find_notes_misskey(url, user_id)
  end

  defp get_posts(url, :mastodon, user_id) do
    find_notes_mastodon(url, user_id)
  end

  def find_notes_misskey(url, user_id) do
    resp = HTTPoison.post(
      url <> "/api/users/notes", 
      %{
        userId: user_id,
        withFiles: true,
        withRenotes: false,
        withBots: false,
        withQuotes: false,
        withReplies: false,
        limit: 100
      } |> Jason.encode!(),
      @headers
    )

    case resp do
      {:ok, %{status_code: 200, body: body}} ->
        notes = body |> Jason.decode!()
        notes = 
          for %{"id" => note_id} <- notes do
            note_id
          end
        note_count = length(notes)

        case note_count do
          100 -> find_notes_misskey(url, user_id, notes)
          _ -> {:ok, notes}
        end

      _ ->
        {:error, :note_request_failed}
    end

  end

  def find_notes_misskey(url, user_id, prev_notes) do
    last_id = List.last(prev_notes)
    :logger.debug("find notes recursive: #{last_id}")

    resp = HTTPoison.post(
      url <> "/api/users/notes", 
      %{
        userId: user_id,
        withFiles: true,
        withRenotes: false,
        withBots: false,
        withQuotes: false,
        withReplies: false,
        untilId: last_id,
        limit: 100
      } |> Jason.encode!(),
      @headers
    )

    case resp do
      {:ok, %{status_code: 200, body: body}} ->
        notes = body |> Jason.decode!()
        notes = 
          for %{"id" => note_id} <- notes do
            note_id
          end
        note_count = length(notes)

        notes = prev_notes ++ notes
        case note_count do
          100 -> find_notes_misskey(url, user_id, notes)
          _ -> {:ok, notes}
        end

      _ ->
        {:error, :note_request_failed}
    end
  end

  def find_notes_mastodon(url, user_id) do
    resp = HTTPoison.get(
      url <> "/api/v1/accounts/#{user_id}/statuses?only_media=true&exclude_replies=true",
      @headers
    )

    case resp do
      {:ok, %{status_code: 200, body: body}} ->
        notes = body |> Jason.decode!()
        notes = 
          for %{"id" => note_id} <- notes do
            note_id
          end
        note_count = length(notes)

        case note_count do
          20 -> find_notes_mastodon(url, user_id, notes)
          _ -> {:ok, notes}
        end

      _ ->
        {:error, :note_request_failed}
    end
  end

  def find_notes_mastodon(url, user_id, prev_notes) do
    last_id = List.last(prev_notes)
    :logger.debug("find notes recursive: #{last_id}")
    resp = HTTPoison.get(
      url <> "/api/v1/accounts/#{user_id}/statuses?only_media=true&exclude_replies=true&max_id=#{last_id}",
      @headers
    )

    case resp do
      {:ok, %{status_code: 200, body: body}} ->
        notes = body |> Jason.decode!()
        notes = 
          for %{"id" => note_id} <- notes do
            note_id
          end
        note_count = length(notes)

        notes = prev_notes ++ notes
        case note_count do
          20 -> find_notes_mastodon(url, user_id, notes)
          _ -> {:ok, notes}
        end

      _ ->
        {:error, :note_request_failed}
    end
  end

  def post_id_to_note_url(notes, url, instance_type, user_id) do
    case instance_type do
      :mastodon ->
        for note <- notes do
          "#{url}/users/#{user_id}/statuses/#{note}"
        end
      :misskey ->
        for note <- notes do
          "#{url}/notes/#{note}"
        end
    end
  end
  
end
