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

  ```elixir
  %{
    aliases: %{},
    assocs: [],
    combinations: [],
    distinct: nil,
    from: %Ecto.Query.FromExpr{
      as: nil,
      hints: [],
      prefix: nil,
      source: {"users", Schema.User}
    },
    group_bys: [],
    havings: [],
    joins: [],
    limit: nil,
    lock: nil,
    offset: nil,
    order_bys: [],
    prefix: nil,
    preloads: [],
    select: nil,
    sources: nil,
    updates: [],
    wheres: [
      %Ecto.Query.BooleanExpr{
        expr: {:in, [], [{{:., [], [{:&, [], [0]}, :id]}, [], []}, {:^, [], [0]}]},
        file: "iex",
        line: 7,
        op: :and,
        params: [{[12, 14, 16], {:in, {0, :id}}}]
      },
      %Ecto.Query.BooleanExpr{
        expr: {:==, [],
        [
          {{:., [], [{:&, [], [0]}, :id]}, [], []},
          %Ecto.Query.Tagged{tag: nil, type: {0, :id}, value: 3}
        ]},
        file: "iex",
        line: 7,
        op: :and,
        params: []
      }
    ],
    windows: []
  }
  ```
  """

  alias Decimal, as: D

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
        |> fetch_shard_ids_from_where_query(key)
        |> case do
          {:ok, []} ->
            do_all(queryable, opts, :all)

          {:ok, ids} ->
            do_all(queryable, opts, ids)
        end
      end

      defp all(queryable, opts, _) when is_list(opts),
        do: do_all(queryable, opts, :all)

      defp do_all(queryable, opts, :all),
        do: all_shard_repos() |> Enum.flat_map(& &1.all(queryable, opts))

      defp do_all(queryable, opts, [id]), do: calculate_target_repo(id).all(queryable, opts)

      defp do_all(queryable, opts, ids) when is_list(ids) do
        ids
        |> Enum.map(&calculate_target_repo/1)
        |> Enum.uniq()
        |> Enum.flat_map(& &1.all(queryable, opts))
      end

      #
      # delete
      #

      @doc """
      Deletes a struct using its primary key.

      Delegates to `Ecto.Repo.Delete` after deciding which Repo to look up.
      The target Repo will be looked up with the key given by `:#{@shard_key_optname}`.
      This option is **required**. If that information is not known,
      you should use `delete_all/2` instead.

      It returns `{:ok, struct}` if the struct has been successfully
      deleted from the Repo, or `{:error, changeset}` if there was a
      validation or a known constraint error.
      See `Ecto.Repo.delete/2` for errors.
      """
      def delete(%_{} = struct_or_changeset, opts \\ []) do
        target_repo = fetch_target_repo!(struct_or_changeset, opts)
        target_repo.delete(struct_or_changeset, opts)
      end

      def delete!(%_{} = struct_or_changeset, opts \\ []) do
        target_repo = fetch_target_repo!(struct_or_changeset, opts)
        target_repo.delete!(struct_or_changeset, opts)
      end

      #
      # delete_all
      #

      @doc """
      Deletes all entries matching the given query.

      It returns a tuple containing the number of entries and any returned
      result as second element. The second element is `nil` by default
      unless a `select` is supplied in the update query. Note, however,
      not all databases support returning data from DELETEs.

      ## Options

        * `:prefix` - The prefix to run the query on (such as the schema path
          in Postgres or the database in MySQL). This overrides the prefix set
          in the query and any `@schema_prefix` set in the schema.

      See the "Shared options" section at the module documentation for
      remaining options.

      ## Examples

          MyRepo.delete_all(Post)

          from(p in Post, where: p.id < 10) |> MyRepo.delete_all
      """
      @spec delete_all(queryable :: Ecto.Queryable.t(), opts :: Keyword.t()) ::
              {integer, nil | [term]}
      def delete_all(queryable, opts \\ []) do
        result =
          ShardRepoHelper.do_in_transaction(
            all_shard_repos(),
            fn repo -> repo.delete_all(queryable, opts) end,
            opts
          )

        with {:ok, results} <- result do
          results
          |> Enum.reduce({0, []}, fn
            {i, nil}, {acc_i, acc_list} -> {acc_i + i, acc_list}
            {i, list}, {acc_i, acc_list} -> {acc_i + i, acc_list ++ list}
          end)
          |> case do
            {i, []} -> {i, nil}
            with_list -> with_list
          end
        else
          {:error, _reasons} ->
            raise "delete_all for shards failed, but not returing :error tuple in sake of implementing callbacks"
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
      # Aggregate
      #

      @spec aggregate(Ecto.Queryable.t(), aggregate, atom, Keyword.t()) ::
              {:ok, number | Decimal.t()} | {:error, [term]}
            when aggregate: :count | :avg | :max | :min | :sum
      def aggregate(queryable, aggregate, field, opts \\ [])
          when aggregate in [:count, :avg, :max, :min, :sum] and is_atom(field) and is_list(opts) do
        ShardRepoHelper.do_in_transaction(
          all_shard_repos(),
          fn repo -> repo.aggregate(queryable, aggregate, field, opts) end,
          opts
        )
        |> case do
          {:ok, results} -> {:ok, results |> finalize_aggregate(aggregate)}
          {:error, _} = error -> error
        end
      end

      defp finalize_aggregate(results, :count), do: Enum.sum(results)
      defp finalize_aggregate(results, :avg), do: Enum.sum(results) / length(results)
      defp finalize_aggregate(results, :max), do: Enum.max(results)
      defp finalize_aggregate(results, :min), do: Enum.min(results)

      defp finalize_aggregate(results, :sum),
        do:
          results
          |> Enum.reduce(0, fn
            nil, acc -> acc
            x, acc -> D.add(x, acc)
          end)

      def transaction(fun_or_multi, opts \\ []) do
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

      # IDはintegerだったりUUIDだったりするのでbinaryもある
      # 前の定義↓
      # {:ok, integer | binary | nil | list} | {:error, :not_found} | :error
      # リストを返すことにしたのでerrorを廃止してみる
      # => where句がない場合全DBに検索かけることになってしまうがしかたない？
      @spec fetch_shard_ids_from_where_query(Ecto.Query.t(), atom) ::
              [integer] | [binary] | []
      defp fetch_shard_ids_from_where_query(%Ecto.Query{} = query, sharding_key) do
        query
        |> Map.get(:wheres)
        |> do_fetch_shard_ids_from_where_query(sharding_key)
      end

      @doc """
      ## 構造体
          %Ecto.Query{
            ...,
            wheres: [
              %Ecto.Query.BooleanExpr{
                expr: {:in, [], [{{:., [], [{:&, [], [0]}, :id]}, [], []}, {:^, [], [0]}]},
                file: "iex",
                line: 7,
                op: :and,
                params: [{[12, 14, 16], {:in, {0, :id}}}]
              },
              %Ecto.Query.BooleanExpr{
                expr: {:==, [],
                [
                  {{:., [], [{:&, [], [0]}, :id]}, [], []},
                  %Ecto.Query.Tagged{tag: nil, type: {0, :id}, value: 3}
                ]},
                file: "iex",
                line: 7,
                op: :and,
                params: []
              }
            ],
          }
      """
      # defp do_fetch_shard_ids_from_where_query([], _), do: {:error, :not_found}

      defp do_fetch_shard_ids_from_where_query(wheres, sharding_key) do
        wheres
        |> Enum.flat_map(fn
          %Ecto.Query.BooleanExpr{
            expr: {:in, _, [{{_, _, [_, ^sharding_key]}, _, _}, _]},
            params: [{values, _}]
          } ->
            values

          %Ecto.Query.BooleanExpr{
            expr: {:==, _, [_, %{type: {:array, {_, ^sharding_key}}, value: val}]}
          } ->
            [val]

          %Ecto.Query.BooleanExpr{expr: {:==, _, [_, %{type: {_, ^sharding_key}, value: val}]}} ->
            [val]

          _ ->
            []
        end)

        # |> Enum.find_value(fn
        #   %{expr: {_, _, [_, %{type: {_, ^sharding_key}, value: val}]}} -> {:ok, val}
        #   %{expr: {_, _, [_, %{type: {:array, {_, ^sharding_key}}, value: val}]}} -> {:ok, val}
        #   _ -> :error
        # end)
      end
    end
  end
end
