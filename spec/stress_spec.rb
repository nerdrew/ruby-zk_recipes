# frozen_string_literal: true

RSpec.describe "stress test", zookeeper: true, proxy: true, stress: true do
  let(:logger) { Logger.new(ENV["ZK_DEBUG"] ? STDERR : nil) }
  let!(:cache) do
    cache = ZkRecipes::Cache.new(logger: logger)
    cache.register("/test/boom", "boom")
    cache.register("/test/foo", "foo")
    cache.setup_callbacks(zk_proxy)
    zk_proxy.connect
    cache.wait_for_warm_cache(timeout)
    cache
  end

  let(:zk_proxy) do
    ZK.new("localhost:#{PROXY_PORT}", connect: false, timeout: timeout).tap do |z|
      z.on_exception { |e| zk_cache_exceptions << e.class }
    end
  end

  let(:zk) do
    ZK.new("localhost:#{ZK_PORT}").tap do |z|
      z.on_exception { |e| zk_exceptions << e.class }
    end
  end

  let(:delays) { [] }
  let(:versions) { { "/test/boom" => [], "/test/foo" => [] } }
  let(:seconds) { 120 }
  let(:timeout) { 5 }
  let(:zk_cache_exceptions) { Set.new }
  let(:zk_exceptions) { Set.new }
  let(:children) { [] }

  before do
    ActiveSupport::Notifications.subscribe("zk_recipes.cache.update") do |_name, _start, _finish, _id, payload|
      delays << payload[:latency_seconds]
      versions[payload[:path]] << payload[:version]
    end
  end

  after do
    ActiveSupport::Notifications.unsubscribe("zk_recipes.cache.update")
    zk.close!
    cache.close!
    zk_proxy.close!
    expect(zk_exceptions).to be_empty
    expect(zk_cache_exceptions).to be_empty
  end

  def update_values
    @expected_version ||= 0
    @expected_version += 1
    zk.set("/test/boom", "boom")
    zk.set("/test/foo", "foo")
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
      begin
        logger.debug("child reopen")
        zk_proxy.reopen

        # exit successfully if an update is received
        ActiveSupport::Notifications.subscribe("zk_recipes.cache.update") do |*_args|
          next unless zk_cache_exceptions.empty? && zk_exceptions.empty?
          logger.debug("child succeeded pid=#{Process.pid}")
          exit!(0)
        end

        sleep 10
        warn "child failed pid=#{Process.pid} "\
          "zk_cache_exceptions=#{zk_cache_exceptions.inspect} "\
          "zk_exceptions=#{zk_exceptions.inspect}"
        exit!(1)
      end
    end
  end

  it "works" do
    stop = Time.now + seconds
    threads = Array.new(5) do
      Thread.new do
        i = 0
        loop do
          cache["/test/boom"]
          cache["/test/foo"]
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
    zk.create("/test/foo", "foo")

    until Time.now > stop
      warn "update_values" if ENV["ZK_DEBUG"]
      10.times { update_values }
      warn "slow proxy" if ENV["ZK_DEBUG"]
      slow_proxy
      warn "update_values" if ENV["ZK_DEBUG"]
      10.times { update_values }
      warn "expire_session" if ENV["ZK_DEBUG"]
      expire_session
      warn "fork" if ENV["ZK_DEBUG"]
      test_fork
    end

    children.each do |pid|
      _, status = Process.wait2(pid)
      expect(status.exitstatus).to eq(0) # exitstatus == 0 => tests passed in this child
    end

    warn "Stress Test Summary"
    warn "delay mean=#{delays.mean.round(3)}s median=#{delays.median.round(3)}s "\
      "99percentile=#{delays.percentile(90).round(3)}s max=#{delays.max.round(3)}s min=#{delays.min.round(3)}s"
    warn "read/ms=" + threads.map { |t| t.join.value / seconds / 1_000 }.inspect
    warn "last_expected_version=#{@expected_version}"
    warn "versions=#{versions.inspect}"
  end
end
