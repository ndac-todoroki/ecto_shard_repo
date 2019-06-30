defmodule EctoShardRepo do
  @moduledoc """
  ## やりたいこと
  どのRepoを使うかを判定してからEcto.Repoをuseしているやつに委譲したい

  ```elixir
  defmodule UserRepo
    use ShardRepo,
      shard_repos: [],
      shard_function: fn -> end
  end

  def list_users do
    User
    |> UserRepo.all()
  end
  ```

  ## 注意
  - (Schemaで) メインテーブルでは `autogenerate: true` をしないこと
    - changeset作成の関数の中でメインテーブル全体のidを作成する関数をさしておく。
    - user_idの最新を持ってるDBがshardしてないところにある、とか

  ## TODO
  ↓みたいな感じでクエリからキーが取れるはず

  ```elixir
  Schema.UserChar
  |> where(user_id: 12)
  |> Map.get(:wheres)
  |> Enum.find_value(fn
    %{expr: {_, _, [_, %{type: {_, :user_id}, value: val}]}} -> val
    _ -> nil
  end)
  # => 12
  ```

  これは `Ecto.Query` の構造体
  """

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Ecto.Repo

      {repos_map, function} = ShardRepoHelper.compile_config(opts)

      @shard_repos_map repos_map
      @shard_function function
      @shard_key_optname :shard_for

      #
      # all
      #

      @doc """
      Returns all items which match the query.

      ## Options
      - `:#{@shard_key_optname}` - The key of the value to shard with, given as an `atom`.
      If this option is passed, `all/2` tries to shard by fetching the shard value from the given queryable.
      If the option wasn't passed, `all/2` will pass the request to all the defined shards.

      ## Examples
      ```elixir
      User |> Repo.all()
      User |> where(user_id: 15) |> Repo.all(shard_key: :user_id)
      ```
      """
      def all(queryable, opts \\ []), do: all(queryable, opts, opts |> Enum.into(%{}))

      defp all(queryable, opts, %{@shard_key_optname => key})
           when is_list(opts) and is_atom(key) do
        queryable
        |> Ecto.Queryable.to_query()
        |> fetch_shard_id_from_where_query(key)
        |> case do
          {:ok, nil} ->
            do_all(queryable, opts, :all)

          {:ok, id_or_ids} ->
            do_all(queryable, opts, id_or_ids)

          {:error, :not_found} ->
            raise """
            No queries found with `:#{key}` in use! \
            Consider removing the `:#{@shard_key_optname}` option.
            """

          :error ->
            raise "parse error at `defp all when is_list(opts)`"
        end
      end

      defp all(queryable, opts, _) when is_list(opts),
        do: do_all(queryable, opts, :all)

      defp do_all(queryable, opts, :all),
        do: all_shard_repos() |> Enum.flat_map(& &1.all(queryable, opts))

      defp do_all(queryable, opts, ids) when is_list(ids) do
        ids
        |> Enum.map(&calculate_target_repo/1)
        |> Enum.uniq()
        |> Enum.flat_map(& &1.all(queryable, opts))
      end

      defp do_all(queryable, opts, id), do: calculate_target_repo(id).all(queryable, opts)

      #
      # delete
      #

      def delete(struct_or_changeset, opts \\ []) do
        target_repo = fetch_target_repo!(struct_or_changeset, opts)
        target_repo.delete(struct_or_changeset, opts)
      end

      def delete!(struct_or_changeset, opts \\ []) do
        target_repo = fetch_target_repo!(struct_or_changeset, opts)
        target_repo.delete!(struct_or_changeset, opts)
      end

      @spec delete_all(queryable :: Ecto.Queryable.t(), opts :: Keyword.t()) ::
              {integer, nil | [term]}
      def delete_all(queryable, opts \\ []) do
        all_shard_repos()
        |> Enum.map(& &1.delete_all(queryable, opts))
        |> Enum.reduce({0, []}, fn
          {i, nil}, {acc_i, acc_list} -> {acc_i + i, acc_list}
          {i, list}, {acc_i, acc_list} -> {acc_i + i, acc_list ++ list}
        end)
        |> case do
          {i, []} -> {i, nil}
          with_list -> with_list
        end
      end

      @spec exists?(queryable :: Ecto.Queryable.t(), opts :: Keyword.t()) :: boolean()
      def exists?(queryable, opts \\ []) do
        all_shard_repos()
        |> Enum.any?(& &1.exists?(queryable, opts))
      end

      @doc """
      Finds an entry by a given id and a sharding value.

      ## Added Options
      - `:shard_value` An id for sharding.

      ## Examples

          iex> user = User |> Repo.all() |> List.first()
          iex> UserPosts |> Repo.get(13, shard_value: user.id)

      """
      @spec get(queryable :: Ecto.Queryable.t(), id :: term(), opts :: Keyword.t()) ::
              Ecto.Schema.t() | nil
      def get(queryable, id, opts \\ []) do
        repo = opts |> fetch_target_repo_by_given_value!(:shard_value)
        repo.get(queryable, id, opts)
      end

      @spec get!(queryable :: Ecto.Queryable.t(), id :: term(), opts :: Keyword.t()) ::
              Ecto.Schema.t()
      def get!(queryable, id, opts \\ []) do
        repo = opts |> fetch_target_repo_by_given_value!(:shard_value)
        repo.get!(queryable, id, opts)
      end

      @spec get_by(
              queryable :: Ecto.Queryable.t(),
              clauses :: Keyword.t() | map(),
              opts :: Keyword.t()
            ) :: Ecto.Schema.t() | nil
      def get_by(queryable, clauses, opts \\ []) do
        repo = opts |> fetch_target_repo_by_given_value!(:shard_value)
        repo.get_by(queryable, clauses, opts)
      end

      @spec get_by!(
              queryable :: Ecto.Queryable.t(),
              clauses :: Keyword.t() | map(),
              opts :: Keyword.t()
            ) :: Ecto.Schema.t()
      def get_by!(queryable, clauses, opts \\ []) do
        repo = opts |> fetch_target_repo_by_given_value!(:shard_value)
        repo.get_by!(queryable, clauses, opts)
      end

      @spec insert(
              struct_or_changeset :: Ecto.Schema.t() | Ecto.Changeset.t(),
              opts :: Keyword.t()
            ) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
      def insert(struct_or_changeset, opts \\ []) do
        target_repo = fetch_target_repo!(struct_or_changeset, opts)
        target_repo.insert(struct_or_changeset, opts)
      end

      @spec insert!(
              struct_or_changeset :: Ecto.Schema.t() | Ecto.Changeset.t(),
              opts :: Keyword.t()
            ) :: Ecto.Schema.t()
      def insert!(struct_or_changeset, opts \\ []) do
        target_repo = fetch_target_repo!(struct_or_changeset, opts)
        target_repo.insert!(struct_or_changeset, opts)
      end

      @spec insert_all(
              schema_or_source :: binary() | {binary(), module()} | module(),
              entries :: [map() | [{atom(), term() | Ecto.Query.t()}]],
              opts :: Keyword.t()
            ) :: {integer(), nil | [term()]}
      def insert_all(schema_or_source, entries, opts \\ []) do
        shard_key = opts |> Keyword.fetch!(@shard_key_optname)

        entries
        |> Enum.group_by(fn
          entry when entry |> is_map ->
            entry |> Map.fetch!(shard_key) |> calculate_target_repo()

          entry when entry |> is_list ->
            entry |> Keyword.fetch!(shard_key) |> calculate_target_repo()
        end)
        |> Enum.flat_map(fn {repo, grouped_entries} ->
          grouped_entries |> Enum.map(&repo.insert_all(schema_or_source, &1))
        end)
        |> Enum.reduce({0, []}, fn
          {i, nil}, {acc_i, acc_list} -> {acc_i + i, acc_list}
          {i, list}, {acc_i, acc_list} -> {acc_i + i, acc_list ++ list}
        end)
        |> case do
          {i, []} -> {i, nil}
          with_list -> with_list
        end
      end

      @spec one(queryable :: Ecto.Queryable.t(), opts :: Keyword.t()) ::
              Ecto.Schema.t() | nil
      def one(queryable, opts \\ []) do
        repo = opts |> fetch_target_repo_by_given_value!(:shard_value)
        repo.one(queryable, opts)
      end

      @spec one!(queryable :: Ecto.Queryable.t(), opts :: Keyword.t()) :: Ecto.Schema.t()
      def one!(queryable, opts \\ []) do
        repo = opts |> fetch_target_repo_by_given_value!(:shard_value)
        repo.one!(queryable, opts)
      end

      #
      # Helpers
      #

      defp all_shard_repos do
        @shard_repos_map |> Map.values()
      end

      defp calculate_target_repo(shard_value) do
        repo_key = @shard_function.(shard_value)
        @shard_repos_map |> Map.fetch!(repo_key)
      end

      defp fetch_target_repo!(struct_or_changeset, opts \\ []) do
        key = opts |> Keyword.fetch!(@shard_key_optname)

        struct_or_changeset
        |> Map.fetch!(key)
        |> calculate_target_repo()
      end

      @spec fetch_target_repo_by_given_value!(Keyword.t(), atom) :: term
      defp fetch_target_repo_by_given_value!(opts, keyname) do
        opts
        |> Keyword.fetch!(keyname)
        |> calculate_target_repo()
      end

      @spec fetch_shard_id_from_where_query(Ecto.Query.t(), atom) ::
              {:ok, integer | binary | nil | list} | {:error, :not_found} | :error
      defp fetch_shard_id_from_where_query(%Ecto.Query{} = query, sharding_key) do
        query
        |> Map.get(:wheres)
        |> do_fetch_shard_id_from_where_query(sharding_key)
      end

      defp do_fetch_shard_id_from_where_query([], _), do: {:error, :not_found}

      defp do_fetch_shard_id_from_where_query(wheres, sharding_key) do
        wheres
        |> Enum.find_value(fn
          %{expr: {_, _, [_, %{type: {_, ^sharding_key}, value: val}]}} -> {:ok, val}
          %{expr: {_, _, [_, %{type: {:array, {_, ^sharding_key}}, value: val}]}} -> {:ok, val}
          _ -> :error
        end)
      end
    end
  end
end
