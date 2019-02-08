# frozen_string_literal: true

RSpec.describe ZkRecipes::Cache, zookeeper: true do
  let(:logger) do
    TestLogger.new((ENV["ZK_RECIPES_DEBUG"].present? ? STDERR : nil), formatter: Formatter.new)
  end
  let(:closers) { [] }
  let(:host) { |example| "localhost:#{example.metadata[:proxy] ? PROXY_PORT : ZK_PORT}" }
  let(:cache) do
    ZkRecipes::Cache.new(host: host, logger: logger, timeout: 10, zk_opts: { timeout: 5 }) do |z|
      z.register("/test/boom", "goat")
      z.register("/test/foo", 1) { |raw_value| raw_value.to_i * 2 }
      z.register_children("/test/group", ["/test/foo"]) { |children| children.map { |child| "/test/#{child}" } }
      closers << z
    end.tap do |z|
      z.register_runtime("/test/bar", "oatmeal")
      z.register_runtime("/test/yogurt", 10) { |raw_value| raw_value.to_i * 2 }
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

  let(:runtime_events) { [] }
  let(:runtime_sub) do
    ActiveSupport::Notifications.subscribe("runtime_cache.zk_recipes") do |_name, _start, _finish, _id, payload|
      runtime_events << payload
    end
  end

  let(:children_events) { [] }
  let(:children_sub) do
    ActiveSupport::Notifications.subscribe("children_cache.zk_recipes") do |_name, _start, _finish, _id, payload|
      children_events << payload
    end
  end

  after do
    closers.each(&:close!)
    ActiveSupport::Notifications.unsubscribe("cache.zk_recipes")
    ActiveSupport::Notifications.unsubscribe("runtime_cache.zk_recipes")
    ActiveSupport::Notifications.unsubscribe("children_cache.zk_recipes")
    expect(TestLogger.errors.reject { |msg| msg =~ /process_pending_updates: failed to process pending updates/ }).to eq([])
    TestLogger.errors.clear
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
      expect(events[1][:old_value]).to eq("goat")
      expect(events[1][:path]).to eq("/test/boom")
      expect(events[1][:value]).to eq("cat")
      expect(events[1][:version]).to eq(0)
    end
  end

  describe "#register_runtime" do
    it "raises if called within an initialization block" do
      expect {
        ZkRecipes::Cache.new(host: host, logger: logger, timeout: 10, zk_opts: { timeout: 5 }) do |cache|
          cache.register_runtime("/test/blamo", "cat")
        end
      }.to raise_error(described_class::Error)
    end

    it "raises if called before setup_callbacks" do
      cache = ZkRecipes::Cache.new
      closers << cache
      expect { cache.register_runtime("/test/blamo", "cat") }.to raise_error(described_class::Error)
    end

    it "raises if called twice with the same path" do
      cache.register_runtime("/test/blamo", "cat")
      expect { cache.register_runtime("/test/blamo", "dog") }.to raise_error(described_class::RuntimeRegistrationError)
    end

    it "raises if called with an already registered path" do
      expect { cache.register_runtime("/test/boom", "dog") }.to raise_error(described_class::RuntimeRegistrationError)
    end

    it "receives updates after on_connect when registered before on_connect" do
      z = ZK.new(host, connect: false)
      cache = ZkRecipes::Cache.new
      closers << cache
      cache.setup_callbacks(z)
      cache.register_runtime("/test/blamo", "cat")
      zk.create("/test/blamo", "dog")

      # the cache's zk client isn't connected yet
      expect(cache.fetch_runtime("/test/blamo")).to eq("cat")
      z.connect
      cache.wait_for_warm_cache(5)
      expect(cache.fetch_runtime("/test/blamo")).to eq("dog")
    end

    it "does not block on IO", proxy: true, throttle_bytes_per_sec: 100 do
      cache
      expect(Benchmark.realtime { cache.register_runtime("/test/blamo", "cat") }).to be < 0.001
    end

    it "fires an AS::Notifications with the default value and when the path is updated" do
      runtime_sub
      zk.create("/test/bar", "cat")
      almost_there { expect(cache.fetch_runtime("/test/bar")).to eq("cat") }

      events = runtime_events.select { |payload| payload[:path] == "/test/bar" }
      expect(events.size).to eq(2)
      expect(events[0]).to eq(path: "/test/bar", value: "oatmeal")
      expect(events[1][:data_length]).to eq(3)
      expect(events[1][:latency_seconds]).to be < 1
      expect(events[1][:old_value]).to eq("oatmeal")
      expect(events[1][:path]).to eq("/test/bar")
      expect(events[1][:value]).to eq("cat")
      expect(events[1][:version]).to eq(0)
    end
  end

  describe "#unregister_runtime" do
    it "works" do
      almost_there { expect(cache.fetch_runtime("/test/yogurt")).to eq(10) }
      cache.unregister_runtime("/test/yogurt")
      expect { cache.fetch_runtime("/test/yogurt") }.to raise_error(ZkRecipes::Cache::PathError)
    end

    it "errors for bad paths" do
      expect { cache.unregister_runtime("/test/no_path") }.to raise_error(ZkRecipes::Cache::PathError)
    end
  end

  describe "#register_children" do
    it "raises if called after initialization with a block" do
      cache = ZkRecipes::Cache.new(host: host, logger: logger, timeout: 10, zk_opts: { timeout: 5 }) { |_| }
      closers << cache
      expect { cache.register_children("/test/blamo", ["cat"]) }.to raise_error(described_class::Error)
    end

    it "raises if called after setup_callbacks" do
      z = ZK.new(host, connect: false)
      cache = ZkRecipes::Cache.new
      closers << cache
      cache.setup_callbacks(z)
      expect { cache.register_children("/test/blamo", ["cat"]) }.to raise_error(described_class::Error)
    end

    it "raises if called twice with the same path" do
      expect {
        ZkRecipes::Cache.new(host: host, logger: logger, timeout: 10, zk_opts: { timeout: 5 }) do |z|
          z.register_children("/test/blamo", ["cat"])
          z.register_children("/test/blamo", ["dog"])
        end
      }.to raise_error(described_class::Error)
    end

    it "fires an AS::Notifications with the default value and when the path is updated" do
      children_sub
      zk.mkdir_p("/test/group/boom")
      almost_there { expect(cache.fetch_children("/test/group")).to eq(["/test/boom"]) }

      events = children_events.select { |payload| payload[:path] == "/test/group" }
      expect(events.size).to eq(2)
      expect(events[0]).to eq(path: "/test/group", value: ["/test/foo"])
      expect(events[1][:children_version]).to eq(1)
      expect(events[1][:data_length]).to eq(0)
      expect(events[1][:latency_seconds]).to be < 1
      expect(events[1][:old_value]).to eq(["/test/foo"])
      expect(events[1][:path]).to eq("/test/group")
      expect(events[1][:value]).to eq(["/test/boom"])
      expect(events[1][:version]).to eq(0)

      zk.mkdir_p("/test/group/bar")
      almost_there { expect(children_events.last[:children_version]).to eq(2) }
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

  describe "#fetch_runtime" do
    it "fetches runtime registered paths" do
      expect(cache.fetch_runtime("/test/bar")).to eq("oatmeal")
      zk.create("/test/bar", "cat")
      almost_there { expect(cache.fetch_runtime("/test/bar")).to eq("cat") }
    end

    it "raises a PathError for unregistered paths" do
      expect { cache.fetch_runtime("/test/baz") }.to raise_error(described_class::PathError)
    end
  end

  describe "#fetch_runtime_valid" do
    it "fetches registered paths that are set" do
      zk.create("/test/bar", "cat")
      almost_there { expect(cache.fetch_runtime_valid("/test/bar")).to eq("cat") }
    end

    it "returns nil for unset paths" do
      expect(cache.fetch_runtime_valid("/test/bar")).to be(nil)
    end

    context "paths with deserialization errors" do
      let(:cache) do
        ZkRecipes::Cache.new(host: host, logger: logger, timeout: 10, zk_opts: { timeout: 5 }) { |_| }.tap do |z|
          closers << z
          z.register_runtime("/test/bar", "goat") { |_| raise "never valid" }
        end
      end

      it "returns nil" do
        expect(cache.fetch_runtime_valid("/test/bar")).to be(nil)
        zk.create("/test/bar", "cat")
        almost_there { expect(cache.fetch_runtime_valid("/test/bar")).to be(nil) }
        expect_logged_errors(/never valid/)
      end
    end

    it "raises PathError for unregistered paths" do
      expect { cache.fetch_valid("/test/baz") }.to raise_error(described_class::PathError)
    end
  end

  describe "#fetch_children" do
    it "fetches the children of registered paths" do
      expect(cache.fetch_children("/test/group")).to eq(["/test/foo"])
      zk.mkdir_p("/test/group/boom")
      almost_there { expect(cache.fetch_children("/test/group")).to eq(["/test/boom"]) }
    end

    it "raises a PathError for unregistered paths" do
      expect { cache.fetch_children("/test/baz") }.to raise_error(described_class::PathError)
    end
  end

  describe "#fetch_children_valid" do
    it "fetches children of registered paths that are set" do
      zk.mkdir_p("/test/group/boom")
      almost_there { expect(cache.fetch_children_valid("/test/group")).to eq(["/test/boom"]) }
    end

    it "returns nil for unset paths" do
      expect(cache.fetch_children_valid("/test/group")).to be(nil)
    end

    context "paths with deserialization errors" do
      let(:cache) do
        ZkRecipes::Cache.new(host: host, logger: logger, timeout: 10, zk_opts: { timeout: 5 }) do |z|
          closers << z
          z.register_children("/test/group", "/test/foo") { |_| raise "never valid" }
        end
      end

      it "returns nil" do
        expect(cache.fetch_children_valid("/test/group")).to be(nil)
        zk.mkdir_p("/test/group/boom")
        almost_there { expect(cache.fetch_children_valid("/test/group")).to be(nil) }
        expect_logged_errors(/never valid/)
      end
    end

    it "raises PathError for unregistered paths" do
      expect { cache.fetch_children_valid("/test/baz") }.to raise_error(described_class::PathError)
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
      expect(cache.fetch_runtime("/test/bar")).to eq("oatmeal")
      expect(cache.fetch_runtime("/test/yogurt")).to eq(10)
      expect(cache.fetch_children("/test/group")).to eq(["/test/foo"])

      zk.create("/test/boom", "cat")
      almost_there { expect(cache["/test/boom"]).to eq("cat") }

      zk.create("/test/foo", "1")
      almost_there { expect(cache["/test/foo"]).to eq(2) }

      zk.create("/test/bar", "cat")
      almost_there { expect(cache.fetch_runtime("/test/bar")).to eq("cat") }

      zk.create("/test/yogurt", "2")
      almost_there { expect(cache.fetch_runtime("/test/yogurt")).to eq(4) }

      zk.mkdir_p("/test/group/boom")
      almost_there { expect(cache.fetch_children("/test/group")).to eq(["/test/boom"]) }
    end

    it "works" do
      check_everything
    end

    context "with an external ZK client" do
      let(:zk_cache) { ZK.new(host, connect: false).tap { |z| closers << z } }

      let(:cache) do
        cache = ZkRecipes::Cache.new(logger: logger)
        closers << cache
        cache.register("/test/boom", "goat")
        cache.register("/test/foo", 1) { |raw_value| raw_value.to_i * 2 }
        cache.register_children("/test/group", ["/test/foo"]) { |children| children.map { |child| "/test/#{child}" } }
        cache.setup_callbacks(zk_cache)
        zk_cache.connect
        cache.register_runtime("/test/bar", "oatmeal")
        cache.register_runtime("/test/yogurt", 10) { |raw_value| raw_value.to_i * 2 }
        cache
      end

      it "works" do
        check_everything
      end
    end

    describe "flakey connections", proxy: true do
      it "works with slow connections", throttle_bytes_per_sec: 100 do
        check_everything
      end

      it "works with dropped connections" do
        expect(cache["/test/boom"]).to eq("goat")
        zk.create("/test/boom", "cat")
        almost_there { expect(cache["/test/boom"]).to eq("cat") }

        expect(cache.fetch_runtime("/test/bar")).to eq("oatmeal")
        zk.create("/test/bar", "cat")
        almost_there { expect(cache.fetch_runtime("/test/bar")).to eq("cat") }

        expect(cache.fetch_children("/test/group")).to eq(["/test/foo"])
        zk.mkdir_p("/test/group/boom")
        almost_there { expect(cache.fetch_children("/test/group")).to eq(["/test/boom"]) }

        proxy_stop
        sleep 1

        zk.set("/test/boom", "dog")
        expect(cache["/test/boom"]).to eq("cat")

        zk.set("/test/bar", "dog")
        expect(cache.fetch_runtime("/test/bar")).to eq("cat")

        zk.create("/test/group/foo")
        expect(cache.fetch_children("/test/group")).to eq(["/test/boom"])

        sleep 12
        proxy_start
        sleep 1

        almost_there { expect(cache["/test/boom"]).to eq("dog") }
        almost_there { expect(cache.fetch_runtime("/test/bar")).to eq("dog") }
        almost_there { expect(cache.fetch_children("/test/group")).to eq(["/test/boom", "/test/foo"]) }
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
            cache.register_children("/test/group", ["/test/foo"]) { |children| children.map { |child| "/test/#{child}" } }
            cache.setup_callbacks(zk_cache)
            zk_cache.connect
            cache.wait_for_warm_cache(5)
            cache.register_runtime("/test/bar", "oatmeal")
            cache.register_runtime("/test/yogurt", 10) { |raw_value| raw_value.to_i * 2 }
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
                expect(cache.wait_for_warm_cache(1)).to be(false)
                expect(cache.wait_for_warm_cache(10)).to be(true)
                almost_there { expect(cache["/test/boom"]).to eq("cat") }
                almost_there { expect(cache["/test/foo"]).to eq(2) }
                almost_there { expect(cache.fetch_runtime("/test/bar")).to eq("cat") }
                almost_there { expect(cache.fetch_runtime("/test/yogurt")).to eq(4) }
                almost_there { expect(cache.fetch_children("/test/group")).to eq(["/test/boom"]) }
                expect_logged_errors(/didn't warm cache before timeout connected=true timeout=1/)
                exit!(0)
              rescue Exception => e
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
              z.register_children("/test/group", ["/test/foo"]) { |children| children.map { |child| "/test/#{child}" } }
            end.tap do |z|
              z.register_runtime("/test/bar", "oatmeal")
              z.register_runtime("/test/yogurt", 10) { |raw_value| raw_value.to_i * 2 }
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
                almost_there { expect(cache.fetch_runtime("/test/bar")).to eq("cat") }
                almost_there { expect(cache.fetch_runtime("/test/yogurt")).to eq(4) }
                almost_there { expect(cache.fetch_children("/test/group")).to eq(["/test/boom"]) }
                raise "zk_cache_exceptions=#{TestLogger.errors.inspect}" unless TestLogger.errors.empty?
                exit!(0)
              rescue Exception => e
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
