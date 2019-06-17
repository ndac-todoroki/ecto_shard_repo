defmodule EctoShardRepoTest do
  use ExUnit.Case
  doctest EctoShardRepo

  test "greets the world" do
    assert EctoShardRepo.hello() == :world
  end
end
