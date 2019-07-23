#!/usr/bin/env ruby
# frozen_string_literal: true

require "bundler/setup"
require "zk_recipes"
require "zk"
require "zk-server"

begin
  ZK_PORT = (27183..28_000).detect { |port| !system("nc -z localhost #{port}") }

  ZK::Server.run do |c|
    c.client_port = ZK_PORT
    c.force_sync = false
    c.snap_count = 1_000_000
    c.max_session_timeout = 5_000 # ms
  end

  zk = ZK.new("localhost:#{ZK_PORT}")
  zk.rm_rf("/test")
  zk.mkdir_p("/test")

  # A cache that creates its own ZK client:

  cache = ZkRecipes::Cache.new(host: "localhost:#{ZK_PORT}", timeout: 10, zk_opts: { timeout: 5 }) do |z|
    z.register("/test/boom", "goat") { |raw_value| raw_value.upcase }
    z.register("/test/foo", 1) { |raw_value| raw_value.to_i * 2 }
    z.register_directory("/test/groupA", ->(child) { "/test/#{child}" }) do |raw_value|
      "#{raw_value}s!"
    end
    z.register_directory("/test/groupB", ->(child) { "/test/#{child}" }) do |raw_value|
      "#{raw_value}?"
    end
  end

  # Get the default value for /test/boom
  puts "/test/boom = #{cache["/test/boom"].inspect}" # => "goat"
  # Set /test/boom
  zk.create("/test/boom", "maaa")
  # Wait a jiffy for the cache to get the change from ZK
  sleep 0.1
  # Get the updated value for /test/boom from the cache
  puts "/test/boom = #{cache["/test/boom"].inspect}" # => "MAAA"

  zk.mkdir_p("/test/groupA/runtime")
  sleep 0.1
  # There's one watched child of directory, but the mapped path (/test/runtime) is not set.
  puts "/test/groupA values = #{cache.fetch_directory_values("/test/groupA").inspect}" # => {}

  # Add /test/boom to groupA
  zk.mkdir_p("/test/groupA/boom")
  # Set /test/runtime, which is in groupA
  zk.create("/test/runtime", "mooo")

  sleep 0.1

  # There are now two watched children and both are set.
  puts "/test/groupA values = #{cache.fetch_directory_values("/test/groupA").inspect}"
  # => { "/test/boom" => "MAAA", "/test/runtime" => "mooos!" }

  # Add paths to groupB
  zk.mkdir_p("/test/groupB/boom")
  zk.mkdir_p("/test/groupB/runtime")

  sleep 0.1

  puts "/test/groupB values = #{cache.fetch_directory_values("/test/groupB").inspect}"
  # => {
  #   # Static paths (paths that are registered at config time) return the parsed value for that registered path.
  #   "/test/boom" => "MAAA",
  #   # Runtime paths (paths that are NOT registered at config time) use the parser configured for the directory.
  #   "/test/runtime" => "mooo?"
  # }

  zk.set("/test/runtime", "meow")

  sleep 0.1

  puts "/test/groupA values = #{cache.fetch_directory_values("/test/groupA").inspect}"
  # => { "/test/boom" => "MAAA", "/test/runtime" => "meows!" }
  puts "/test/groupB values = #{cache.fetch_directory_values("/test/groupB").inspect}"
  # => { "/test/boom" => "MAAA", "/test/runtime" => "meow?" }

  cache.close!
ensure
  ZK::Server.shutdown
  sleep 0.1
  FileUtils.rm_r(File.expand_path('../../zookeeper', __FILE__))
end
