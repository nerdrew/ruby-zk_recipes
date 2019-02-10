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
    z.register("/test/boom", "goat")
    z.register("/test/foo", 1) { |raw_value| raw_value.to_i * 2 }
    z.register_children("/test/groupA", ["/test/foo"]) do |children|
      children
        .map { |child| "/test/#{child}" }
        .each { |path| cache.register_runtime(path, "") unless cache.registered_runtime_path?(path) || cache.registered_path?(path) }
    end
  end

  cache.register_runtime("/test/bar", "cat")

  ActiveSupport::Notifications.subscribe("children_cache.zk_recipes") do |_name, _start, _finish, _id, payload|
    if payload[:error]
      p payload[:error]
    elsif payload[:path] == "/test/groupA"
      (payload[:old_value] - payload[:value]).each do |stale_child_path|
        next unless cache.registered_runtime_path?(stale_child_path)

        puts "Removing stale child's runtime path=#{stale_child_path.inspect}"
        cache.unregister_runtime(stale_child_path)
      end
    end
  end

  # Get the default value for /test/boom
  puts "/test/boom = #{cache["/test/boom"].inspect}" # => "goat"
  # Set /test/boom
  zk.create("/test/boom", "maaa")
  # Wait a jiffy for the cache to get the change from ZK
  sleep 0.1
  # Get the updated value for /test/boom from the cache
  puts "/test/boom = #{cache["/test/boom"].inspect}" # => "maaa"

  puts "/test/bar (runtime path) = #{cache.fetch_runtime("/test/bar").inspect}" # => "cat"
  zk.create("/test/bar", "dog")
  sleep 0.1
  puts "/test/bar (runtime path) = #{cache.fetch_runtime("/test/bar").inspect}" # => "dog"

  # We haven't registered anything for /test/mooo, it raises an error...
  begin
    cache.fetch_runtime("/test/mooo")
  rescue ZkRecipes::Cache::PathError => e
    puts e.inspect
  end
  puts "/test/groupA = #{cache.fetch_children("/test/groupA").inspect}" # => ["/test/foo"]
  zk.mkdir_p("/test/groupA/mooo")
  sleep 0.1
  puts "/test/groupA = #{cache.fetch_children("/test/groupA").inspect}" # => ["/test/mooo"]

  # The callback for /test/groupA registers a runtime path for any children; e.g. /test/mooo in this case
  puts "/test/mooo (runtime path registered by the /test/groupA callback) = #{cache.fetch_runtime("/test/mooo").inspect}" # => ""
  zk.create("/test/mooo", "cow")
  sleep 0.1
  puts "/test/mooo (runtime path registered by the /test/groupA callback) = #{cache.fetch_runtime("/test/mooo")}" # => "cow"

  # Remove /test/groupA/mooo child, the associated runtime path will be remove by the ActiveSupport::Notifications subscription
  zk.delete("/test/groupA/mooo")
  sleep 0.1
  begin
    cache.fetch_runtime("/test/mooo")
  rescue ZkRecipes::Cache::PathError => e
    puts e.inspect # There is once again, no /test/mooo
  end

  cache.close!
ensure
  ZK::Server.shutdown
  sleep 0.1
  FileUtils.rm_r(File.expand_path('../../zookeeper', __FILE__))
end
