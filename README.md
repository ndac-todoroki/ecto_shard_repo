# EctoShardRepo

Enable Ecto queries to sharded databases.

This is a work in progress, while also an experiment.

## Installation

This is not published for now, so `git clone` this and write the below in your `mix.exs`:

```elixir
defp deps do
  [
    ...,
    {:ecto_shard_repo, path: "local/path/to/ecto_shard_repo"},
  ]
end
```

## Usage

```elixir
## concrete Repo files

defmodule ShardedRepo001 do
  use Ecto.Repo,
      otp_app: :testplay_datapicker,
      adapter: Ecto.Adapters.MyXQL
end

defmodule ShardedRepo002 do
  use Ecto.Repo,
      otp_app: :testplay_datapicker,
      adapter: Ecto.Adapters.Postgres
end

## config.exs

config :your_app, ShardedRepo001,
  database: "sharded_repo_001_development",
  username: "...",
  password: "...",
  hostname: "localhost",
  pool_size: 10

config :your_app, ShardedRepo002,
  database: "sharded_repo_002_development",
  username: "...",
  password: "...",
  hostname: "localhost",
  pool_size: 10

## abstract Repo file

defmodule MyRepo do
  @shards [
    ShardedRepo001,
    ShardedRepo002,
  ]

  use EctoShardedRepo,
      shard_repos: @shards,
      shard_function: &__MODULE__.shard_function/1

  def shard_function(key) do
    rem(key, Enum.count(@shards))
  end
end
```

Then you can use like:

```elixir
## defining :shard_key will make selective access work
User
|> where([u], u.id in [1, 3, 5, 7])
|> MyRepo.all(shard_key: :id)

"""
12:11:28.599 [debug] QUERY OK source="users" db=1.7ms decode=1.6ms queue=10.5ms
SELECT u0.`id`, u0.`id`, u0.`name` FROM `users` AS u0 WHERE (u0.`id` IN (1,3,5,7)) []
[
  %Schema.User{
   __meta__: #Ecto.Schema.Metadata<:loaded, "users">,
    id: 1,
    name: "Tom"
  },
  ...
"""

## if you don't, it will simply pass the query to all shards.
User
|> where([u], u.id in [1, 3, 5, 7])
|> MyRepo.all()

"""
12:11:28.599 [debug] QUERY OK source="users" db=1.7ms decode=1.6ms queue=10.5ms
SELECT u0.`id`, u0.`id`, u0.`name` FROM `users` AS u0 WHERE (u0.`id` IN (1,3,5,7)) []

12:11:28.601 [debug] QUERY OK source="users" db=1.7ms decode=1.6ms queue=1.0ms
SELECT u0.`id`, u0.`id`, u0.`name` FROM `users` AS u0 WHERE (u0.`id` IN (1,3,5,7)) []
[
  %Schema.User{
   __meta__: #Ecto.Schema.Metadata<:loaded, "users">,
    id: 1,
    name: "Tom"
  },
  ...
"""
```

_note: Repo selection by passing `:shard_key` to the opts list might be removed. See issue #2_

### Non Round-Robbin databases
You can use snowflake ids or any other uuids with `EctoShardRepo`.
The abstract Repo module would be like: about 

```elixir
## abstract Repo file

defmodule MyRepo do
  # use key-value pairs for :shard_repos
  @shards [
    amaryllis: ShardedRepo001,
    buttercup: ShardedRepo002,
    ...
  ]
  @typep repo_key :: :amaryllis | :buttercup | ...

  use EctoShardedRepo,
      shard_repos: @shards,
      shard_function: &__MODULE__.resolve_repo_key/1

  @spec resolve_repo_key(uuid :: String.t) :: repo_key()
  def shard_function(uuid) do
    Snowflake.resolve_key_from_uuid(uuid)
  end
end
```

This way it won't be a mess when you add new databases and `Repo`s to your shard group, because you can edit the function to keep old uuids not to point to the new `Repo`s.

## Cross-Repo Transactions

`EctoShardingRepo` uses message based transactions in certain queries, such as `delete_all/3`.
This is done by the following instructions:

1. Open `Ecto.Repo.transaction/3` in all concrete `Repo`s which were called, and makes
2. Make them `send` (in means of process messaging) `:ok` or `:error` according to its operations success inside the transaction
3. The caller (`EctoShardRepo`) waits for all children to send back messages
4. `case` all returned message were :ok `do`  
     true -> send :success to all children  
     false -> send :rollback to all children  
   `end`
5. Children waits for incoming messages, and simply quits on `:success`, or raise and `rollback` on `:rollback`.
6. The caller aggregates all the results and returns it

This way,
- Instructions/Operations could rollback if any of them fail in any of the shard databases.
- The entire transaction/operation will take the same time length as the slowest database run.
- (I don't see any benefits, but) multiple database adaptors could be used together (MyXQL, Postgres, etc. for sharding same types of data)

Cons about this method is
- The message size could be huge
  - message from children contains results of the query run
  - huge messages could be a bottleneck?
  - multiple message could be a bottleneck?
- All databases will be locked until the slowest query finishes

If there are any other points (Pros/Cons), please comment freely on a issue.
