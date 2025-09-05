defmodule FishySync do
  use Application

  def start(_, _) do
    {pg_port, _r} = System.fetch_env!("PG_PORT") |> Integer.parse()

    base_cfg = [
      hostname: System.fetch_env!("PG_HOST"),
      port: pg_port,
      database: System.fetch_env!("PG_DATABASE"),
      username: System.fetch_env!("PG_USER"),
      password: System.fetch_env!("PG_PASSWORD"),
      tcp_keepalives_idle: 10
    ]

    db_cfg =  [name: FishySync.Database] ++ base_cfg
    notif_cfg =  [name: FishySync.Notifications] ++ base_cfg

    children = [
      {Postgrex.Notifications, notif_cfg},
      {Postgrex, db_cfg},
      {SyncCore, []}
    ]

    opts = [
      strategy: :one_for_one,
      name: FishySync.Supervisor
    ]

    Supervisor.start_link(children, opts)
  end
end
