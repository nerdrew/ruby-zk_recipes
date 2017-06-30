# frozen_string_literal: true

RSpec.describe ZkRecipes::Cache, zookeeper: true do
  let(:logger) { Logger.new(STDERR).tap { |l| l.level = ENV["ZK_RECIPES_DEBUG"] ? Logger::DEBUG : Logger::WARN } }
  let(:host) { "localhost:#{ZK_PORT}" }
  let(:cache) do
    ZkRecipes::Cache.new(host: host, logger: logger, timeout: 10, zk_opts: { timeout: 5 }) do |z|
      z.register("/test/boom", "goat")
      z.register("/test/foo", 1) { |raw_value| raw_value.to_i * 2 }
    end
  end

  let(:zk) do
    ZK.new("localhost:#{ZK_PORT}").tap do |z|
      z.on_exception { |e| zk_exceptions << e.class }
    end
  end
  let(:zk_exceptions) { Set.new }
  let(:zk_cache_exceptions) { Set.new }

  after do
    zk.close!
    cache.close!
    expect(zk_exceptions.empty?).to be(true)
    expect(zk_cache_exceptions.empty?).to be(true)
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
      cache.setup_callbacks(z)
      expect(Benchmark.realtime { cache.wait_for_warm_cache(5) } >= 5).to be(true)
      expect(z.connected?).to be(false)
    end
  end

  describe "#fetch" do
    it "fetches registered paths" do
      expect(cache["/test/boom"]).to eq("goat")
      expect(cache.fetch("/test/boom")).to eq("goat")
    end

    it "raises a KeyError for unregistered paths" do
      expect { cache["/test/baz"] }.to raise_error KeyError
    end
  end

  describe "#fetch_existing" do
    it "fetches registered paths that are set" do
      zk.create("/test/boom")
      zk.set("/test/boom", "cat")
      almost_there { expect(cache.fetch_existing("/test/boom")).to eq("cat") }
    end

    it "returns nil for unset paths" do
      expect(cache.fetch_existing("/test/boom")).to be(nil)
    end

    it "raises KeyError for unregistered paths" do
      expect { cache.fetch_existing("/test/baz") }.to raise_error KeyError
    end
  end

  it "works" do
    expect(cache["/test/boom"]).to eq("goat")
    expect(cache["/test/foo"]).to eq(1)

    zk.create("/test/boom")
    zk.set("/test/boom", "cat")
    almost_there { expect(cache["/test/boom"]).to eq("cat") }

    zk.create("/test/foo")
    zk.set("/test/foo", "1")
    almost_there { expect(cache["/test/foo"]).to eq(2) }
  end

  context "with an external ZK client" do
    let(:zk_cache) { ZK.new(host, connect: false) }
    after { zk_cache.close! }

    let(:cache) do
      cache = ZkRecipes::Cache.new(logger: logger)
      cache.register("/test/boom", "goat")
      cache.register("/test/foo", 1) { |raw_value| raw_value.to_i * 2 }
      cache.setup_callbacks(zk_cache)
      zk_cache.connect
      cache
    end

    it "works" do
      expect(cache["/test/boom"]).to eq("goat")
      expect(cache["/test/foo"]).to eq(1)

      zk.create("/test/boom")
      zk.set("/test/boom", "cat")
      almost_there { expect(cache["/test/boom"]).to eq("cat") }

      zk.create("/test/foo")
      zk.set("/test/foo", "1")
      almost_there { expect(cache["/test/foo"]).to eq(2) }
    end
  end

  describe "flakey connections", proxy: true do
    let(:host) { "localhost:#{PROXY_PORT}" }

    context "with slow connections", throttle_bytes_per_sec: 100 do
      it "works" do
        expect(cache["/test/boom"]).to eq("goat")
        expect(cache["/test/foo"]).to eq(1)

        zk.create("/test/boom")
        zk.set("/test/boom", "cat")
        almost_there { expect(cache["/test/boom"]).to eq("cat") }

        zk.create("/test/foo")
        zk.set("/test/foo", "1")
        almost_there { expect(cache["/test/foo"]).to eq(2) }
      end
    end

    it "works with dropped connections" do
      expect(cache["/test/boom"]).to eq("goat")
      zk.create("/test/boom")
      zk.set("/test/boom", "cat")
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
      let(:host) { "localhost:#{PROXY_PORT}" }

      context "with external ZK client" do
        let!(:cache) do
          cache = ZkRecipes::Cache.new(logger: logger)
          cache.register("/test/boom", "goat")
          cache.setup_callbacks(zk_cache)
          zk_cache.connect
          cache.wait_for_warm_cache(5)
          cache
        end

        let(:zk_cache) do
          ZK.new(host, connect: false, timeout: 5).tap do |z|
            z.on_exception { |e| zk_cache_exceptions << e.class }
          end
        end
        after { zk_cache.close! }

        it "works" do
          child_pid = fork
          if child_pid
            # parent
            begin
              zk.create("/test/boom")
              zk.set("/test/boom", "cat")
              almost_there { expect(cache["/test/boom"]).to eq("cat") }

              _, status = Process.wait2(child_pid)
              expect(status.exitstatus).to eq(0) # exitstatus == 0 => tests passed in child
            rescue
              Process.wait(child_pid) rescue nil
              raise
            end
            raise "zk_cache_exceptions=#{zk_cache_exceptions.inspect}" unless zk_cache_exceptions.empty?
            raise "zk_exceptions=#{zk_exceptions.inspect}" unless zk_exceptions.empty?
          else
            # child
            begin
              cache.reopen
              zk_cache.reopen
              expect(cache.wait_for_warm_cache(1)).to be(false)
              expect(cache.wait_for_warm_cache(5)).to be(true)
              expect(cache["/test/boom"]).to eq("cat")
              raise "zk_cache_exceptions=#{zk_cache_exceptions.inspect}" unless zk_cache_exceptions.empty?
              raise "zk_exceptions=#{zk_exceptions.inspect}" unless zk_exceptions.empty?
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
            z.register("/test/boom", "goat")
          end
        end

        it "works" do
          child_pid = fork
          if child_pid
            # parent
            begin
              zk.create("/test/boom")
              zk.set("/test/boom", "cat")
              almost_there { expect(cache["/test/boom"]).to eq("cat") }

              _, status = Process.wait2(child_pid)
              expect(status.exitstatus).to eq(0) # exitstatus == 0 => tests passed in child
            rescue
              Process.wait(child_pid) rescue nil
              raise
            end
            raise "zk_cache_exceptions=#{zk_cache_exceptions.inspect}" unless zk_cache_exceptions.empty?
            raise "zk_exceptions=#{zk_exceptions.inspect}" unless zk_exceptions.empty?
          else
            # child
            begin
              cache.reopen
              expect(cache["/test/boom"]).to eq("cat")
              raise "zk_cache_exceptions=#{zk_cache_exceptions.inspect}" unless zk_cache_exceptions.empty?
              raise "zk_exceptions=#{zk_exceptions.inspect}" unless zk_exceptions.empty?
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
