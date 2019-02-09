# frozen_string_literal: true

module ZkRecipes
  class Cache
    class Error < StandardError; end
    class PathError < Error; end

    AS_NOTIFICATION = "cache.zk_recipes"
    USE_DEFAULT = Object.new

    def initialize(logger: nil, host: nil, timeout: nil, zk_opts: {})
      @cache = Concurrent::Map.new
      @latch = Concurrent::CountDownLatch.new
      @logger = logger
      @pending_updates = Concurrent::Hash.new # Concurrent::Map does not implement #reject!
      @registerable = true
      @registered_values = Concurrent::Map.new
      @session_id = nil
      @watches = Concurrent::Map.new
      @zk = nil

      if block_given?
        @owned_zk = true
        @warm_cache_timeout = timeout || 30
        yield(self)

        expiration = Time.now + @warm_cache_timeout
        connect(host, zk_opts)

        wait_for_warm_cache(expiration - Time.now)
      elsif host || timeout || !zk_opts.empty?
        raise ArgumentError, "host, zk_opts, and timeout are only allowed with a block"
      else
        @owned_zk = false
      end
    end

    def register(path, default_value, &block)
      raise Error, "register only allowed before setup_callbacks called" unless @registerable

      @cache[path] = CachedPath.new(default_value)
      @registered_values[path] = RegisteredPath.new(default_value, block)
      @logger&.debug { "added path=#{path} default_value=#{default_value.inspect}" }
      ActiveSupport::Notifications.instrument(AS_NOTIFICATION, path: path, value: default_value)
    end

    def setup_callbacks(zk_client)
      raise Error, "setup_callbacks can only be called once" unless @registerable
      @zk = zk_client
      @registerable = false

      if @zk.connected? || @zk.connecting?
        raise Error, "the ZK::Client is already connected, the cached values must be set before connecting"
      end

      @registered_values.each do |path, _value|
        @watches[path] = @zk.register(path) do |event|
          if event.node_event?
            @logger&.debug { "node event path=#{event.path} #{event.event_name} #{event.state_name}" }
            unless update_cache(event.path)
              @pending_updates[path] = nil
              @zk.defer { process_pending_updates }
            end
          else
            @logger&.warn { "session event #{event.event_name} #{event.state_name}" }
          end
        end
      end

      @watches["on_connected"] = @zk.on_connected do
        if @session_id == @zk.session_id
          @logger&.debug("on_connected: reconnected existing session")
          process_pending_updates
          next
        end

        @logger&.debug("on_connected: new session")
        @pending_updates.clear
        @registered_values.each do |path, _value|
          @pending_updates[path] = nil unless update_cache(path)
        end
        @session_id = @zk.session_id
        @latch.count_down
      end

      @zk.on_exception do |e|
        @logger&.error { "on_exception: exception=#{e.inspect} backtrace=#{e.backtrace.inspect}" }
      end
    end

    def wait_for_warm_cache(timeout = 30)
      @logger&.debug { "waiting for cache to warm timeout=#{timeout.inspect}" }
      if @latch.wait(timeout)
        true
      else
        @logger&.warn { "didn't warm cache before timeout connected=#{@zk.connected?} timeout=#{timeout.inspect}" }
        false
      end
    end

    def close!
      @watches.each_value(&:unsubscribe)
      @zk.close! if @owned_zk
      @watches.clear
      @pending_updates.clear
    end

    # reopen the client after the process forks
    # This is *not* the opposite of `#close!`.
    def reopen
      @latch = Concurrent::CountDownLatch.new
      @session_id = nil
      @pending_updates.clear
      if @owned_zk
        expiration = Time.now + @warm_cache_timeout
        @zk.reopen
        wait_for_warm_cache(expiration - Time.now)
      end
    end

    def fetch(path)
      @cache.fetch(path).value
    rescue KeyError
      raise PathError, "no registered path for #{path.inspect}"
    end
    alias_method :[], :fetch

    def fetch_valid(path)
      cached = @cache.fetch(path)
      cached.value if cached.valid?
    rescue KeyError
      raise PathError, "no registered path=#{path.inspect}"
    end

    private

    def connect(host, zk_opts)
      raise Error, "already connected" if @zk

      @logger&.debug { "connecting host=#{host.inspect}" }
      ZK.new(host, **zk_opts) do |zk|
        setup_callbacks(zk)
      end
    end

    # only called from ZK thread
    def update_cache(path)
      stat = @zk.stat(path, watch: true)

      instrument_params = { path: path }

      unless stat.exists?
        value = @registered_values.fetch(path).default_value
        @cache[path] = CachedPath.new(value, stat: stat)
        @logger&.debug { "update_cache: no data node, setting watch path=#{path}" }
        instrument_params[:value] = value
        ActiveSupport::Notifications.instrument(AS_NOTIFICATION, instrument_params)
        return true
      end

      raw_value, stat = @zk.get(path, watch: true)

      instrument_params[:latency_seconds] = Time.now - stat.mtime_t
      instrument_params[:version] = stat.version
      instrument_params[:data_length] = stat.data_length

      valid = true
      value = begin
        registered_value = @registered_values.fetch(path)
        instrument_params[:value] = registered_value.deserialize(raw_value)
      rescue => e
        @logger&.error { "deserialization error path=#{path} stat=#{stat.inspect} exception=#{e.inspect} #{e.backtrace.inspect}" }
        instrument_params[:error] = e
        instrument_params[:raw_value] = raw_value
        valid = false
        registered_value.default_value
      end

      if value == USE_DEFAULT
        valid = false
        value = registered_value.default_value
      end

      @cache[path] = CachedPath.new(value, stat: stat, valid: valid)

      ActiveSupport::Notifications.instrument(AS_NOTIFICATION, instrument_params)
      @logger&.debug { "update_cache: path=#{path} raw_value=#{raw_value.inspect} value=#{value.inspect} stat=#{stat.inspect}" }
      true
    rescue ::ZK::Exceptions::ZKError => e
      @logger&.warn { "update_cache: path=#{path} exception=#{e.inspect}, retrying" }
      retry
    rescue ::ZK::Exceptions::KeeperException, ::Zookeeper::Exceptions::ZookeeperException => e
      @logger&.error { "update_cache: path=#{path} exception=#{e.inspect}" }
      false
    end

    def process_pending_updates
      if @pending_updates.empty?
        @logger&.debug("process_pending_updates: no pending updates")
        return
      end

      unless @zk.connected?
        @logger&.debug("process_pending_updates: zk not connected")
        return
      end

      @logger&.debug { "processing pending updates=#{@pending_updates.size}" }
      @pending_updates.reject! do |missed_path, _|
        update_cache(missed_path)
      end
    end

    class CachedPath
      attr_reader :value, :stat
      def initialize(value, stat: nil, valid: false)
        @value = value
        @stat = stat
        @valid = valid
      end

      def valid?
        @valid
      end
    end
    private_constant :CachedPath

    class RegisteredPath < Struct.new(:default_value, :deserializer)
      def deserialize(raw)
        deserializer ? deserializer.call(raw) : raw
      end
    end
    private_constant :RegisteredPath
  end
end
