defmodule Database do
  defp query(query) do
    Postgrex.query(FishySync.Database, query)
  end
  
  defp query_fmt(query) do
    {:ok, result} = query(query)

    for row <- result.rows do
      row_to_obj(result.columns, row)
    end
  end

  defp row_to_obj(columns, row) do
    if length(columns) != length(row) do
      {:error, :mismatched_column_count}
    else
      row_to_obj(columns, row, %{})
    end
  end

  defp row_to_obj([next_col | cols], [next_row | rows], acc) do
    acc = acc |> Map.put(next_col, next_row)
    row_to_obj(cols, rows, acc)
  end

  defp row_to_obj([], [], acc) do acc end

  def initialize() do
    {:ok, _} = query("""
      CREATE OR REPLACE FUNCTION fs_new_following() RETURNS TRIGGER AS $$
      BEGIN
        PERFORM pg_notify('fishysync_notice', row_to_json(NEW)::text);
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    """)

    {:ok, _} = query("""
      CREATE OR REPLACE TRIGGER fishy_sync_following_insert AFTER INSERT ON following
      FOR EACH ROW
      EXECUTE FUNCTION fs_new_following(NEW);
    """)
  end

  def get_role_id_by_name(name) do
    #FIXME: see if binding can be done
    [%{"id" => id}] = query_fmt("SELECT id FROM role WHERE Name = '#{name}'")
    id
  end

  def user_is_member_of?(user_id, role_id) do
    result = query("""
      SELECT id FROM role_assignment WHERE "userId" = '#{user_id}' AND "roleId" = '#{role_id}'
    """)

    case result do
      {:ok, %{rows: [_]}} -> true
      _ -> false
    end
  end

  def get_username(user_id) do
    {:ok, %{rows: [[
      username, host
    ]]}} = query("""
        SELECT username, host FROM public.user WHERE id = '#{user_id}'
        """)

    {username, host}
  end

  def get_user_id_by_username(fully_qualified_username) do
    [username, host] = String.split(fully_qualified_username, "@")
    #FIXME: see if binding can be done
    [%{"id" => id}] = query_fmt("SELECT id FROM public.user WHERE username = '#{username}' AND host = '#{host}'")
    id
  end

end
