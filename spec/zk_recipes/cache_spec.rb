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

  after do
    closers.each(&:close!)
    expect(TestLogger.errors).to eq([])
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

      zk.create("/test/boom", "cat")
      almost_there { expect(cache["/test/boom"]).to eq("cat") }

      zk.create("/test/foo", "1")
      almost_there { expect(cache["/test/foo"]).to eq(2) }
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
        cache.setup_callbacks(zk_cache)
        zk_cache.connect
        cache
      end

      it "works" do
        check_everything
      end
    end

    describe "flakey connections", proxy: true do
      context "with slow connections", throttle_bytes_per_sec: 100 do
        it "works" do
          check_everything
        end
      end

      it "works with dropped connections" do
        expect(cache["/test/boom"]).to eq("goat")
        zk.create("/test/boom", "cat")
        almost_there { expect(cache["/test/boom"]).to eq("cat") }

        proxy_stop
        sleep 1

        zk.set("/test/boom", "dog")
        expect(cache["/test/boom"]).to eq("cat")

        sleep 12
        proxy_start
        sleep 1

        almost_there { expect(cache["/test/boom"]).to eq("dog") }
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
                expect(cache.wait_for_warm_cache(1)).to be(false)
                expect(cache.wait_for_warm_cache(10)).to be(true)
                almost_there { expect(cache["/test/boom"]).to eq("cat") }
                almost_there { expect(cache["/test/foo"]).to eq(2) }
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
