defmodule SyncWorker do
  use GenServer

  def start_chunk(worker_pid, chunk) do
    GenServer.call(worker_pid, {:start_chunk, chunk})
  end

  def start_link(opt) do
    GenServer.start_link(__MODULE__, opt)
  end

  def init(synchronizer_pid) do
    {:ok, %{
      synchronizer_pid: synchronizer_pid,
      home_host: System.fetch_env!("HOME_HOST"),
      bearer_token: System.fetch_env!("BEARER_TOKEN")
    }}
  end

  def handle_call({:start_chunk, chunk}, _from, state) do
    {:reply, :ok, state, {:continue, chunk}}
  end

  def handle_continue(chunk, %{synchronizer_pid: synchronizer_pid} = state) do
    :logger.info("worker #{inspect self()} starting chunk...")

    headers = [
      {"Content-Type", "application/json"},
      {"Authorization", "Bearer #{state.bearer_token}"}
    ]

    results = for url <- chunk do
      resp = HTTPoison.post(
        "http://127.0.0.1:3000/api/ap/show", 
        %{uri: url} |> Jason.encode!(), 
        headers
      )
      {url, resp}
    end

    for {url, resp} <- results do
      case resp do
        {:error, _} ->
          :logger.warning("#{url} failed, retrying")
          HTTPoison.post(
            "http://127.0.0.1:3000/api/ap/show", 
            %{uri: url} |> Jason.encode!(), 
            headers
          )
        {:ok, %{status_code: 200}} ->
          nil
        _ ->
          :logger.warning("#{url} failed, retrying")
          HTTPoison.post(
            "http://127.0.0.1:3000/api/ap/show", 
            %{uri: url} |> Jason.encode!(), 
            headers
          )
      end
    end


    Synchronizer.report_worker_finish(synchronizer_pid)
    {:noreply, state}
  end

end
