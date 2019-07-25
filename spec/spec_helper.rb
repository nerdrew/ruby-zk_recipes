if ENV["COVERAGE"]
  require "simplecov"

  # Store our original (pre-fork) pid, so that we only call `format!`
  # in our exit handler if we're in the original parent.
  pid = Process.pid
  SimpleCov.at_exit do
    SimpleCov.result.format! if Process.pid == pid
  end

  # Start SimpleCov as usual.
  SimpleCov.start do
    add_filter "/spec/"
  end
end

require "bundler/setup"
require "zk_recipes"

require "descriptive_statistics"
require "logger"
require "pry"
require "zk-server"
require "active_support/core_ext/object/blank"

RSpec.configure do |config|
  class Formatter
    def call(severity, datetime, _progname, msg)
      sprintf(
        "%s %-5s: pid=%d tid=%d %s\n",
        datetime.utc.iso8601(6),
        severity,
        Process.pid,
        Thread.current.object_id,
        msg
      )
    end
  end

  class TestLogger < Logger
    @@errors = Concurrent::Array.new

    def initialize(*)
      super
    end

    def add(sev, *args)
      @@errors << (args.compact.empty? ? yield : args) if sev >= Logger::WARN
      super
    end

    def self.errors
      @@errors
    end
  end

  def expect_logged_errors(*messages)
    almost_there { expect(TestLogger.errors).to match_array(messages) }
    TestLogger.errors.clear
  end

  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.before(:suite) do
    ZK_PORT = (27183..28_000).detect { |port| !system("nc -z localhost #{port}") }
    PROXY_PORT = (ZK_PORT + 1..28_000).detect { |port| !system("nc -z localhost #{port}") }

    ZK::Server.run do |c|
      c.client_port = ZK_PORT
      c.force_sync = false
      c.snap_count = 1_000_000
      c.max_session_timeout = 5_000 # ms
    end
  end

  config.after(:suite) do
    ZK::Server.shutdown
    sleep 0.1
    FileUtils.rm_r(File.expand_path('../../zookeeper', __FILE__))
  end

  config.before(:each, zookeeper: true) do
    ZK.open("localhost:#{ZK_PORT}") do |zk|
      zk.rm_rf("/test")
      zk.mkdir_p("/test")
    end
  end

  config.before(:each, proxy: true) do |group|
    proxy_start(group.metadata[:throttle_bytes_per_sec])
  end

  config.after(:each, proxy: true) do
    proxy_stop
  end

  def proxy_start(throttle_bytes_per_sec = nil)
    limit = "-L #{throttle_bytes_per_sec}" if throttle_bytes_per_sec
    spawn(%{socat -T 10 -d TCP-LISTEN:#{PROXY_PORT},fork,reuseaddr,linger=1 SYSTEM:'pv -q #{limit} - | socat - "TCP:localhost:#{ZK_PORT}"'})
  end

  def proxy_stop
    system("lsof -i TCP:#{PROXY_PORT} -t | grep -v #{Process.pid} | xargs kill -9")
  end

  def spawn(cmd)
    warn "+ #{cmd}" if ENV["ZK_RECIPES_DEBUG"]
    Kernel.spawn(cmd)
  end

  def system(cmd)
    warn "+ #{cmd}" if ENV["ZK_RECIPES_DEBUG"]
    Kernel.system(cmd)
  end

  def almost_there(retries = 100)
    yield
  rescue RSpec::Expectations::ExpectationNotMetError
    raise if retries < 1

    sleep 0.1
    retries -= 1
    retry
  end
end
