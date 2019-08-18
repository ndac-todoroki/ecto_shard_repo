defmodule ShardRepoHelper do
  require IEx
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

  @doc """
  ### Sends
  - `:ok`
  - `{:error, err}`
  """
  @spec shard_transactions(repo, fun | Ecto.Multi.t(), pid, Keyword.t()) :: any
  def shard_transactions(repo, fun_or_multi, caller, options \\ []) do
    repo.transaction(
      fn ->
        try do
          # fun_or_multi.()
          repo.transaction(fun_or_multi, options)
        rescue
          e ->
            # IO.warn(e |> inspect)
            send(caller, {:error, e})
        else
          result ->
            send(caller, {:ok, result})
        end

        receive do
          :noop ->
            # The transaction succeeds.
            :noop

          {:rollback, message} ->
            repo.rollback(message)

          msg ->
            raise "not allowed message type `#{msg |> inspect}` received in #{repo}. Rollbacking..."
        end
      end,
      options
    )
  end

  @spec do_in_transaction([Ecto.Repo.t()], fun | Ecto.Multi.t(), Keyword.t()) ::
          {:ok, results} | {:error, reasons}
        when results: [term], reasons: [term]
  def do_in_transaction(repos, fun_or_multi, opts) do
    me = self()

    tx = fn repo ->
      operations =
        case fun_or_multi do
          lambda when lambda |> is_function(1) ->
            fn -> lambda.(repo) end

          %Ecto.Multi{} = multi ->
            multi
        end

      fn -> shard_transactions(repo, operations, me, opts) end
    end

    receiver = fn _pid ->
      receive do
        {:ok, result} ->
          result

        {:error, err} ->
          # IO.inspect(err)
          {:error, err}
      end
    end

    spawns = Enum.map(repos, &spawn(tx.(&1)))
    results = spawns |> Enum.map(receiver)

    results
    |> Enum.all?(fn result -> match?({:ok, _}, result) end)
    |> case do
      true ->
        spawns |> Enum.each(&send(&1, :noop))
        {:ok, results |> Enum.map(&elem(&1, 1))}

      false ->
        spawns |> Enum.each(&send(&1, {:rollback, :not_all_ok}))

        reasons =
          results
          |> Enum.flat_map(fn
            {:ok, _} -> []
            {:error, reason} -> [reason]
          end)

        {:error, reasons}
    end
  end
end
