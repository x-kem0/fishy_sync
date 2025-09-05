defmodule IexHelpers do
  def sync(username) do
    user_id = Database.get_user_id_by_username(username)
    SyncCore.force_sync(user_id)
  end
end

import IexHelpers
