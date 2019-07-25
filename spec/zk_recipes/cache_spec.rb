# frozen_string_literal: true

RSpec.describe ZkRecipes::Cache, zookeeper: true do
  let(:logger) do
    TestLogger.new((ENV["ZK_RECIPES_DEBUG"].present? ? STDERR : nil), formatter: Formatter.new)
  end
  let(:closers) { [] }
  let(:host) { |example| "localhost:#{example.metadata[:proxy] ? PROXY_PORT : ZK_PORT}" }
  let(:path_mapper) { ->(path) { "/test/#{path}" } }
  let(:cache) do
    ZkRecipes::Cache.new(host: host, logger: logger, timeout: 10, zk_opts: { timeout: 5 }) do |z|
      z.register("/test/boom", "goat")
      z.register("/test/foo", 1) { |raw_value| raw_value.to_i * 2 }
      z.register_directory("/test/group1", path_mapper) { |raw_value| raw_value.to_s + "!" }
      z.register_directory("/test/group2", path_mapper) { |raw_value| raw_value.to_s + "?" }
      z.register_directory("/test/group3", path_mapper) { |raw_value| raw_value.to_s + " :/" }
      closers << z
    end
  end

  let(:zk) do
    ZK.new("localhost:#{ZK_PORT}").tap do |z|
      z.on_exception { |e| logger.error { "on_exception: exception=#{e.inspect} backtrace=#{e.backtrace.inspect}" } }
      closers << z
    end
  end
  let(:zk_cache_exceptions) { Set.new }

  let(:cache_events) { [] }
  let(:cache_sub) do
    ActiveSupport::Notifications.subscribe("cache.zk_recipes") do |_name, _start, _finish, _id, payload|
      cache_events << payload
    end
  end

  let(:directory_events) { [] }
  let(:directory_sub) do
    ActiveSupport::Notifications.subscribe("directory_cache.zk_recipes") do |_name, _start, _finish, _id, payload|
      directory_events << payload
    end
  end

  after do
    closers.each(&:close!)
    ActiveSupport::Notifications.unsubscribe("cache.zk_recipes")
    ActiveSupport::Notifications.unsubscribe("runtime_cache.zk_recipes")
    ActiveSupport::Notifications.unsubscribe("directory_cache.zk_recipes")
    begin
      expect(TestLogger.errors.reject { |msg| msg =~ /process_pending_updates: failed to process pending updates/ }).to eq([])
    ensure
      TestLogger.errors.clear
    end
  end

  describe ".new" do
    it "only takes a host, timeout, or zk_opts with a block" do
      expect { ZkRecipes::Cache.new(host: host) }.to raise_error(ArgumentError)
      expect { ZkRecipes::Cache.new(timeout: 10) }.to raise_error(ArgumentError)
      expect { ZkRecipes::Cache.new(zk_opts: { timeout: 5 }) }.to raise_error(ArgumentError)
    end

    it "sets up callbacks, waits for the cache to warm, but does not connect to ZK without a block" do
      z = ZK.new(host, connect: false)
      cache = ZkRecipes::Cache.new
      closers << cache
      cache.setup_callbacks(z)
      expect(Benchmark.realtime { cache.wait_for_warm_cache(5) } >= 5).to be(true)
      expect(z.connected?).to be(false)
    end
  end

  describe "#register" do
    it "raises if called after initialization with a block" do
      expect { cache.register("/test/blamo", "cat") }.to raise_error(described_class::Error)
    end

    it "raises if called after setup_callbacks" do
      z = ZK.new(host, connect: false)
      cache = ZkRecipes::Cache.new
      closers << cache
      cache.setup_callbacks(z)
      expect { cache.register("/test/blamo", "cat") }.to raise_error(described_class::Error)
    end

    it "raises if called twice with the same path" do
      expect {
        ZkRecipes::Cache.new(host: host, logger: logger, timeout: 10, zk_opts: { timeout: 5 }) do |z|
          z.register("/test/blamo", "cat")
          z.register("/test/blamo", "dog")
        end
      }.to raise_error(described_class::Error)
    end

    # TODO add test for notification for deleted node
    it "fires an AS::Notifications with the default value and when the path is updated" do
      cache_sub
      zk.create("/test/boom", "cat")
      almost_there { expect(cache["/test/boom"]).to eq("cat") }

      events = cache_events.select { |payload| payload[:path] == "/test/boom" }
      expect(events.size).to eq(2)
      expect(events[0]).to eq(path: "/test/boom", value: "goat")
      expect(events[1][:data_length]).to eq(3)
      expect(events[1][:latency_seconds]).to be < 1
      expect(events[1][:path]).to eq("/test/boom")
      expect(events[1][:value]).to eq("cat")
      expect(events[1][:version]).to eq(0)
    end
  end

  describe "#register_directory" do
    it "raises if called after initialization with a block" do
      cache = ZkRecipes::Cache.new(host: host, logger: logger, timeout: 10, zk_opts: { timeout: 5 }) { |_| }
      closers << cache
      expect { cache.register_directory("/test/blamo", path_mapper) }.to raise_error(described_class::Error)
    end

    it "raises if called after setup_callbacks" do
      z = ZK.new(host, connect: false)
      cache = ZkRecipes::Cache.new
      closers << cache
      cache.setup_callbacks(z)
      expect { cache.register_directory("/test/blamo", path_mapper) }.to raise_error(described_class::Error)
    end

    it "raises if called twice with the same path" do
      expect {
        ZkRecipes::Cache.new(host: host, logger: logger, timeout: 10, zk_opts: { timeout: 5 }) do |z|
          z.register_directory("/test/blamo", path_mapper)
          z.register_directory("/test/blamo", path_mapper)
        end
      }.to raise_error(described_class::Error)
    end

    it "fires an AS::Notifications when the path is updated" do
      directory_sub
      zk.mkdir_p("/test/group1/boom")
      almost_there { expect(cache.fetch_directory_values("/test/group1")).to eq("/test/boom" => "goat") }

      events = directory_events.select { |payload| payload[:path] == "/test/group1" }
      expect(events.size).to eq(2)
      expect(events[0]).to eq(path: "/test/group1", directory_paths: [])
      expect(events[1][:directory_version]).to eq(1)
      expect(events[1][:data_length]).to eq(0)
      expect(events[1][:latency_seconds]).to be < 1
      expect(events[1][:path]).to eq("/test/group1")
      expect(events[1][:directory_paths]).to eq(["/test/boom"])
      expect(events[1][:version]).to eq(0)

      zk.mkdir_p("/test/group1/bar")
      almost_there { expect(directory_events.last[:directory_version]).to eq(2) }
    end

    it "watches runtime paths when they are added to the directory and unwatches them when they are removed" do
      zk.mkdir_p("/test/group1/runtime")
      zk.mkdir_p("/test/group1/boom")
      almost_there { expect(cache.fetch_directory_values("/test/group1")).to eq("/test/boom" => "goat") }
      zk.create("/test/runtime", "flower")
      zk.create("/test/boom", "cow")
      almost_there { expect(cache.fetch_directory_values("/test/group1")).to eq("/test/boom" => "cow", "/test/runtime" => "flower!") }
      zk.delete("/test/group1/runtime")
      almost_there { expect(cache.fetch_directory_values("/test/group1")).to eq("/test/boom" => "cow") }
      zk.delete("/test/group1/boom")
      almost_there { expect(cache.fetch_directory_values("/test/group1")).to eq({}) }
      almost_there { expect(cache.fetch("/test/boom")).to eq("cow") }
      zk.delete("/test/boom")
      almost_there { expect(cache.fetch("/test/boom")).to eq("goat") }
      expect(cache.instance_variable_get(:@runtime_watches)["/test/runtime"]).to eq(nil)
    end
  end

  describe "#fetch" do
    it "fetches registered paths" do
      expect(cache["/test/boom"]).to eq("goat")
      expect(cache.fetch("/test/boom")).to eq("goat")
      zk.create("/test/boom", "cat")
      almost_there { expect(cache["/test/boom"]).to eq("cat") }
      almost_there { expect(cache.fetch("/test/boom")).to eq("cat") }
    end

    it "raises a PathError for unregistered paths" do
      expect { cache["/test/baz"] }.to raise_error(described_class::PathError)
    end
  end

  describe "#fetch_valid" do
    it "fetches registered paths that are set" do
      zk.create("/test/boom", "cat")
      almost_there { expect(cache.fetch_valid("/test/boom")).to eq("cat") }
    end

    it "returns nil for unset paths" do
      expect(cache.fetch_valid("/test/boom")).to be(nil)
    end

    context "paths with deserialization errors" do
      let(:cache) do
        ZkRecipes::Cache.new(host: host, logger: logger, timeout: 10, zk_opts: { timeout: 5 }) do |z|
          closers << z
          z.register("/test/boom", "goat") { |_| raise "never valid" }
        end
      end

      it "returns nil" do
        expect(cache.fetch_valid("/test/boom")).to be(nil)
        zk.create("/test/boom", "cat")
        almost_there { expect(cache.fetch_valid("/test/boom")).to be(nil) }
        expect_logged_errors(/never valid/)
      end
    end

    it "raises PathError for unregistered paths" do
      expect { cache.fetch_valid("/test/baz") }.to raise_error(described_class::PathError)
    end
  end

  describe "#fetch_directory_values" do
    it "fetches the values for directory of registered paths" do
      expect(cache.fetch_directory_values("/test/group1")).to eq({})
      zk.create("/test/runtime", "runtime")
      zk.mkdir_p("/test/group1/boom")
      zk.mkdir_p("/test/group1/runtime")
      zk.mkdir_p("/test/group1/unknown")
      almost_there do
        expect(cache.fetch_directory_values("/test/group1")).to eq(
          "/test/boom" => "goat",
          "/test/runtime" => "runtime!",
        )
      end
    end

    it "raises a PathError for unregistered paths" do
      expect { cache.fetch_directory_values("/test/baz") }.to raise_error(described_class::PathError)
    end

    context "paths with deserialization errors" do
      let(:cache) do
        ZkRecipes::Cache.new(host: host, logger: logger, timeout: 10, zk_opts: { timeout: 5 }) do |z|
          closers << z
          z.register_directory("/test/group", path_mapper) { |raw_value| raw_value == "bad" ? raise("never valid") : raw_value }
        end
      end

      it "does not include the paths" do
        almost_there { expect(cache.fetch_directory_values("/test/group")).to eq({}) }
        zk.mkdir_p("/test/group/runtime")
        zk.create("/test/runtime", "good")
        almost_there do
          expect(cache.fetch_directory_values("/test/group")).to eq(
            "/test/runtime" => "good",
          )
        end
        zk.set("/test/runtime", "bad")
        almost_there do
          expect(cache.fetch_directory_values("/test/group")).to eq({})
        end
        expect_logged_errors(/never valid/)
      end
    end

    context "paths with USE_DEFAULT" do
      let(:cache) do
        ZkRecipes::Cache.new(host: host, logger: logger, timeout: 10, zk_opts: { timeout: 5 }) do |z|
          closers << z
          z.register_directory("/test/group", path_mapper) do |raw_value|
            raw_value == "default" ? ::ZkRecipes::Cache::USE_DEFAULT : raw_value
          end
        end
      end

      it "does not include the paths" do
        almost_there { expect(cache.fetch_directory_values("/test/group")).to eq({}) }
        zk.mkdir_p("/test/group/runtime")
        zk.create("/test/runtime", "good")
        almost_there do
          expect(cache.fetch_directory_values("/test/group")).to eq(
            "/test/runtime" => "good",
          )
        end
        zk.set("/test/runtime", "default")
        almost_there do
          expect(cache.fetch_directory_values("/test/group")).to eq({})
        end
      end
    end
  end

  describe "USE_DEFAULT" do
    let(:cache) do
      ZkRecipes::Cache.new(host: host, logger: logger, timeout: 10, zk_opts: { timeout: 5 }) do |z|
        closers << z
        z.register("/test/boom", "goat") { |_| ::ZkRecipes::Cache::USE_DEFAULT }
      end
    end

    it "returns the default value when the deserializer returns USE_DEFAULT" do
      zk.create("/test/boom")
      zk.set("/test/boom", "cat")
      almost_there { expect(zk.get("/test/boom").first).to eq("cat") } # wait for the written value
      expect(cache.fetch("/test/boom")).to eq("goat")
      expect(cache.fetch_valid("/test/boom")).to be(nil)
    end
  end

  describe "acceptance test", slow: true do
    def check_everything
      expect(cache["/test/boom"]).to eq("goat")
      expect(cache["/test/foo"]).to eq(1)
      expect(cache.fetch_directory_values("/test/group1")).to eq({})

      zk.create("/test/boom", "cat")
      almost_there { expect(cache["/test/boom"]).to eq("cat") }

      zk.create("/test/foo", "1")
      almost_there { expect(cache["/test/foo"]).to eq(2) }

      zk.create("/test/bar", "cat")

      zk.create("/test/yogurt", "2")

      zk.create("/test/runtime", "runtime")

      zk.mkdir_p("/test/group1/boom")
      zk.mkdir_p("/test/group1/runtime")
      zk.mkdir_p("/test/group1/unknown")
      zk.mkdir_p("/test/group1/runtime2")
      zk.create("/test/runtime2", "runtime2")
      almost_there do
        expect(cache.fetch_directory_values("/test/group1")).to eq(
          "/test/boom" => "cat",
          "/test/runtime" => "runtime!",
          "/test/runtime2" => "runtime2!",
        )
      end

      zk.mkdir_p("/test/group2/boom")
      zk.mkdir_p("/test/group2/runtime")
      zk.mkdir_p("/test/group2/unknown")
      zk.mkdir_p("/test/group2/runtime2")
      almost_there do
        expect(cache.fetch_directory_values("/test/group2")).to eq(
          "/test/boom" => "cat",
          "/test/runtime" => "runtime?",
          "/test/runtime2" => "runtime2?",
        )
      end
    end

    it "works" do
      check_everything
    end

    describe "flakey connections", proxy: true do
      it "works with slow connections", throttle_bytes_per_sec: 100 do
        check_everything
      end

      it "works with dropped connections" do
        expect(cache["/test/foo"]).to eq(1)
        expect(cache["/test/boom"]).to eq("goat")
        zk.create("/test/boom", "cat")
        almost_there { expect(cache["/test/boom"]).to eq("cat") }

        zk.create("/test/runtime-value-first", "valuey")

        expect(cache.fetch_directory_values("/test/group1")).to eq({})
        zk.mkdir_p("/test/group1/boom")
        zk.create("/test/group1/runtime-group-first")
        almost_there do
          expect(cache.fetch_directory_values("/test/group1")).to eq(
            "/test/boom" => "cat",
          )
        end

        zk.mkdir_p("/test/group2/boom")
        zk.create("/test/group2/runtime-group-first")
        almost_there do
          expect(cache.fetch_directory_values("/test/group2")).to eq(
            "/test/boom" => "cat",
          )
        end

        zk.mkdir_p("/test/group3/runtime3")
        zk.create("/test/runtime3", "don't watch me")
        almost_there do
          expect(cache.fetch_directory_values("/test/group3")).to eq(
            "/test/runtime3" => "don't watch me :/",
          )
        end

        proxy_stop
        sleep 1

        zk.set("/test/boom", "dog")
        zk.create("/test/runtime-group-first", "groupy")
        zk.create("/test/group1/runtime-value-first")
        zk.create("/test/group1/foo")
        zk.create("/test/group2/runtime-value-first")
        zk.create("/test/group2/foo")

        # Remove the dir while offline to make sure an empty directory is handled correctly
        zk.rm_rf("/test/group3")

        expect(cache["/test/boom"]).to eq("cat")
        almost_there do
          expect(cache.fetch_directory_values("/test/group1")).to eq(
            "/test/boom" => "cat",
          )
        end
        almost_there do
          expect(cache.fetch_directory_values("/test/group2")).to eq(
            "/test/boom" => "cat",
          )
        end

        sleep 12
        proxy_start
        sleep 1

        expect(cache["/test/foo"]).to eq(1)
        almost_there { expect(cache["/test/boom"]).to eq("dog") }
        almost_there do
          expect(cache.fetch_directory_values("/test/group1")).to eq(
            "/test/boom" => "dog",
            "/test/foo" => 1,
            "/test/runtime-group-first" => "groupy!",
            "/test/runtime-value-first" => "valuey!",
          )
        end
        almost_there do
          expect(cache.fetch_directory_values("/test/group2")).to eq(
            "/test/boom" => "dog",
            "/test/foo" => 1,
            "/test/runtime-group-first" => "groupy?",
            "/test/runtime-value-first" => "valuey?",
          )
        end

        almost_there do
          expect(cache.fetch_directory_values("/test/group3")).to eq({})
        end
        expect(cache.instance_variable_get(:@runtime_watches)["/test/runtime3"]).to eq(nil)
      end
    end

    context "with an external ZK client" do
      let(:zk_cache) { ZK.new(host, connect: false).tap { |z| closers << z } }

      let(:cache) do
        cache = ZkRecipes::Cache.new(logger: logger)
        closers << cache
        cache.register("/test/boom", "goat")
        cache.register("/test/foo", 1) { |raw_value| raw_value.to_i * 2 }
        cache.register_directory("/test/group1", path_mapper) { |raw_value| raw_value.to_s + "!" }
        cache.register_directory("/test/group2", path_mapper) { |raw_value| raw_value.to_s + "?" }
        cache.setup_callbacks(zk_cache)
        zk_cache.connect
        cache
      end

      it "works" do
        check_everything
      end
    end

    if Process.respond_to?(:fork)
      describe "reopen after fork", proxy: true, throttle_bytes_per_sec: 50 do
        context "with external ZK client" do
          let!(:cache) do
            cache = ZkRecipes::Cache.new(logger: logger)
            closers << cache
            cache.register("/test/boom", "goat")
            cache.register("/test/foo", 1) { |raw_value| raw_value.to_i * 2 }
            cache.register_directory("/test/group1", path_mapper) { |raw_value| raw_value.to_s + "!" }
            cache.register_directory("/test/group2", path_mapper) { |raw_value| raw_value.to_s + "?" }
            cache.setup_callbacks(zk_cache)
            zk_cache.connect
            cache.wait_for_warm_cache(5)
            cache
          end

          let(:zk_cache) do
            ZK.new(host, connect: false, timeout: 5).tap do |z|
              z.on_exception { |e| logger.error { "on_exception: exception=#{e.inspect} backtrace=#{e.backtrace.inspect}" } }
              closers << z
            end
          end

          it "works" do
            child_pid = fork
            if child_pid
              # parent
              begin
                check_everything

                _, status = Process.wait2(child_pid)
                expect(status.exitstatus).to eq(0) # exitstatus == 0 => tests passed in child
              rescue
                Process.wait(child_pid) rescue nil
                raise
              end
              raise "zk_cache_exceptions=#{TestLogger.errors.inspect}" unless TestLogger.errors.empty?
            else
              # child
              begin
                if ENV["COVERAGE"]
                  SimpleCov.command_name SecureRandom.uuid
                  SimpleCov.start
                end

                cache.reopen
                zk_cache.reopen
                almost_there { expect(cache["/test/boom"]).to eq("cat") }
                almost_there { expect(cache["/test/foo"]).to eq(2) }
                almost_there do
                  expect(cache.fetch_directory_values("/test/group1")).to eq(
                    "/test/boom" => "cat",
                    "/test/runtime" => "runtime!",
                    "/test/runtime2" => "runtime2!",
                  )
                end
                almost_there do
                  expect(cache.fetch_directory_values("/test/group2")).to eq(
                    "/test/boom" => "cat",
                    "/test/runtime" => "runtime?",
                    "/test/runtime2" => "runtime2?",
                  )
                end
                exit!(0)
              rescue Exception => e # rubocop:disable Lint/RescueException
                puts "child failed #{e}\n#{e.backtrace.join("\n")}"
                exit!(1)
              end
            end
          end
        end

        context "with internal ZK client (cache creates it's ZK client)" do
          let!(:cache) do
            ZkRecipes::Cache.new(host: host, logger: logger, timeout: 10, zk_opts: { timeout: 5 }) do |z|
              closers << z
              z.register("/test/boom", "goat")
              z.register("/test/foo", 1) { |raw_value| raw_value.to_i * 2 }
              z.register_directory("/test/group1", path_mapper) { |raw_value| raw_value.to_s + "!" }
              z.register_directory("/test/group2", path_mapper) { |raw_value| raw_value.to_s + "?" }
            end
          end

          it "works" do
            child_pid = fork
            if child_pid
              # parent
              begin
                check_everything

                _, status = Process.wait2(child_pid)
                expect(status.exitstatus).to eq(0) # exitstatus == 0 => tests passed in child
              rescue
                Process.wait(child_pid) rescue nil
                raise
              end
              raise "zk_cache_exceptions=#{TestLogger.errors.inspect}" unless TestLogger.errors.empty?
            else
              # child
              begin
                if ENV["COVERAGE"]
                  SimpleCov.command_name SecureRandom.uuid
                  SimpleCov.start
                end

                cache.reopen
                almost_there { expect(cache["/test/boom"]).to eq("cat") }
                almost_there { expect(cache["/test/foo"]).to eq(2) }
                almost_there do
                  expect(cache.fetch_directory_values("/test/group1")).to eq(
                    "/test/boom" => "cat",
                    "/test/runtime" => "runtime!",
                    "/test/runtime2" => "runtime2!",
                  )
                end
                almost_there do
                  expect(cache.fetch_directory_values("/test/group2")).to eq(
                    "/test/boom" => "cat",
                    "/test/runtime" => "runtime?",
                    "/test/runtime2" => "runtime2?",
                  )
                end
                exit!(0)
              rescue Exception => e # rubocop:disable Lint/RescueException
                puts "child failed #{e}\n#{e.backtrace.join("\n")}"
                exit!(1)
              end
            end
          end
        end
      end
    end
  end
end
