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


