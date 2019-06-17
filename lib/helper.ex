defmodule ShardRepoHelper do
  @type repo :: module
  @type shard_repos_map :: %{term => repo}

  @spec compile_config(keyword) :: {shard_repos_map, function}
  def compile_config(opts) do
    {
      opts |> Keyword.fetch!(:shard_repos) |> shard_repos_to_map(),
      opts |> Keyword.fetch!(:shard_function)
    }
  end

  @spec shard_repos_to_map(map | Keyword.t() | [repo]) :: shard_repos_map
  defp shard_repos_to_map(shard_repos) do
    cond do
      shard_repos |> is_map ->
        shard_repos |> Map.values()

      shard_repos |> Keyword.keyword?() ->
        shard_repos |> Enum.into(%{})

      shard_repos |> is_list() ->
        size = shard_repos |> Enum.count()
        0..size |> Enum.zip(shard_repos) |> Enum.into(%{})
    end
  end
end
