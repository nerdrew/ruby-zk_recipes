# frozen_string_literal: true

RSpec.describe "stress test", zookeeper: true, proxy: true, slow: true do
  let(:logger) { Logger.new(ENV["ZK_RECIPES_DEBUG"].present? ? STDERR : nil) }
  let!(:cache) do
    cache = ZkRecipes::Cache.new(logger: logger)
    cache.register("/test/boom", 0, &:to_i)
    cache.setup_callbacks(zk_proxy)
    zk_proxy.connect
    cache.wait_for_warm_cache(timeout)
    cache
  end

  let(:zk_proxy) do
    ZK.new("localhost:#{PROXY_PORT}", connect: false, timeout: timeout * 2).tap do |z|
      z.on_exception { |e| zk_cache_exceptions << e.class }
    end
  end

  let(:zk) do
    ZK.new("localhost:#{ZK_PORT}").tap do |z|
      z.on_exception { |e| zk_exceptions << e.class }
    end
  end

  let(:delays) { [] }
  let(:versions) { [] }
  let(:seconds) { 120 }
  let(:timeout) { 5 }
  let(:zk_cache_exceptions) { Set.new }
  let(:zk_exceptions) { Set.new }
  let(:children) { [] }

  before do
    ActiveSupport::Notifications.subscribe("cache.zk_recipes") do |_name, _start, _finish, _id, payload|
      warn "version mismatch #{payload[:value]} != #{payload[:version]}" if payload[:value] != payload[:version]

      delays << payload[:latency_seconds]
      versions << payload[:value]
    end
  end

  after do
    ActiveSupport::Notifications.unsubscribe("cache.zk_recipes")
    zk.close!
    cache.close!
    zk_proxy.close!
    expect(zk_exceptions).to be_empty
    expect(zk_cache_exceptions).to be_empty
  end

  def update_values
    @expected_version ||= 0
    @expected_version += 1
    zk.set("/test/boom", @expected_version.to_s)
    sleep(rand(0..0.01))
  end

  def slow_proxy
    proxy_stop
    proxy_start(100)
    sleep(rand(0..timeout + 1))
  end

  def expire_session
    proxy_stop
    sleep(rand(timeout + 1..35))
    proxy_start
    sleep(rand(0..timeout + 1))
  end

  def test_fork
    if (child_pid = fork)
      # parent
      children << child_pid
    else
      # child
      logger.debug("child reopen")
      zk_proxy.reopen
      cache.reopen

      # exit successfully if an update is received
      ActiveSupport::Notifications.subscribe("cache.zk_recipes") do |*|
        next unless zk_cache_exceptions.empty? && zk_exceptions.empty?
        logger.debug("child succeeded pid=#{Process.pid}")
        exit!(0)
      end

      almost_there { expect(cache["/test/boom"]).to eq(@expected_version) }

      sleep 10
      warn "child failed pid=#{Process.pid} "\
        "zk_cache_exceptions=#{zk_cache_exceptions.inspect} "\
        "zk_exceptions=#{zk_exceptions.inspect}"
      exit!(1)
    end
  end

  it "works" do
    stop = Time.now + seconds
    threads = Array.new(5) do
      Thread.new do
        i = 0
        loop do
          cache["/test/boom"]
          i += 1
          if i % 1_000 == 0
            break if Time.now > stop
            Thread.pass if RUBY_ENGINE == "ruby"
          end
        end
        i
      end
    end

    sleep(1)
    zk.create("/test/boom", "boom")

    until Time.now > stop
      warn "update_values" if ENV["ZK_RECIPES_DEBUG"]
      10.times { update_values }
      warn "slow proxy" if ENV["ZK_RECIPES_DEBUG"]
      slow_proxy
      warn "update_values" if ENV["ZK_RECIPES_DEBUG"]
      10.times { update_values }
      warn "expire_session" if ENV["ZK_RECIPES_DEBUG"]
      expire_session
      if Process.respond_to?(:fork)
        warn "fork" if ENV["ZK_RECIPES_DEBUG"]
        test_fork
      end
    end

    children.each do |pid|
      _, status = Process.wait2(pid)
      almost_there { expect(status.exitstatus).to eq(0) } # exitstatus == 0 => tests passed in this child
    end

    warn "Stress Test Summary"
    warn "delay mean=#{delays.mean.round(3)}s median=#{delays.median.round(3)}s "\
      "99percentile=#{delays.percentile(90).round(3)}s max=#{delays.max.round(3)}s min=#{delays.min.round(3)}s"
    warn "read/ms=" + threads.map { |t| t.join.value / seconds / 1_000 }.inspect
    warn "versions=#{versions.inspect}"

    almost_there { expect(cache["/test/boom"]).to eq(@expected_version) }
  end

  describe "#register_runtime" do
    it "is threadsafe" do
      cache
      actions = [:register, :unregister, :set, :get]
      paths = [["/test/path1", "value1"], ["/test/path2", "value2"], ["/test/path3", "value3"]]
      paths.each { |path, value| zk.create(path, value) }

      tester = lambda do
        Array.new(100) do |i|
          data = paths.sample
          action = actions.sample

          if action == :set
            zk.set(data[0], "value#{i}")
            next
          end

          Benchmark.realtime do
            case action
            when :register
              begin
                cache.register_runtime(data[0], data[1])
              rescue ZkRecipes::Cache::RuntimeRegistrationError # rubocop:disable Lint/HandleExceptions
              end
            when :unregister
              begin
                cache.unregister_runtime(data[0])
              rescue ZkRecipes::Cache::PathError # rubocop:disable Lint/HandleExceptions
              end
            when :get
              begin
                cache.fetch_runtime(data[0])
              rescue ZkRecipes::Cache::PathError # rubocop:disable Lint/HandleExceptions
              end
            end
          end
        end.compact
      end

      threads = Array.new(10) { Thread.new { tester.call } }
      latencies = tester.call + threads.flat_map { |t| t.join.value }

      warn "#register_runtime / #unregister_runtime / get latencies"
      warn "mean=#{latencies.mean.round(6)}s median=#{latencies.median.round(6)}s "\
        "99percentile=#{latencies.percentile(90).round(6)}s max=#{latencies.max.round(6)}s min=#{latencies.min.round(6)}s"

      expect(latencies.max).to be < 0.1
    end
  end
end
