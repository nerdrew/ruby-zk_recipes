# frozen_string_literal: true

module ZkRecipes
  class Cache
    class Error < StandardError; end

    extend Forwardable

    AS_NOTIFICATION_UPDATE = "zk_recipes.cache.update"
    AS_NOTIFICATION_ERROR = "zk_recipes.cache.error"

    def initialize(host: nil, logger: nil, timeout: nil, zk_opts: {})
      @cache = Concurrent::Map.new
      @latch = Concurrent::CountDownLatch.new
      @logger = logger
      @pending_updates = Concurrent::Hash.new
      @registerable = true
      @registered_values = Concurrent::Map.new
      @session_id = nil
      @watches = Concurrent::Map.new
      @zk = nil

      if block_given?
        @owned_zk = true
        yield(self)

        expiration = Time.now + (timeout || 30)
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

      debug("added path=#{path} default_value=#{default_value.inspect}")
      @cache[path] = CachedPath.new(default_value)
      @registered_values[path] = RegisteredPath.new(default_value, block)
      ActiveSupport::Notifications.instrument(AS_NOTIFICATION_UPDATE, path: path, value: default_value)
    end

    def setup_callbacks(zk)
      @zk = zk
      @registerable = false

      if @zk.connected? || @zk.connecting?
        raise Error, "the ZK::Client is already connected, the cached values must be set before connecting"
      end

      @zk.on_connected do |e|
        info("on_connected session_id old=#{@session_id} new=#{@zk.session_id} #{e.event_name} #{e.state_name}")
        if @session_id == @zk.session_id
          process_pending_updates
          next
        end

        @pending_updates.clear
        @registered_values.each do |path, _value|
          @watches[path] ||= @zk.register(path) do |event|
            if event.node_event?
              debug("node event=#{event.inspect} #{event.event_name} #{event.state_name}")
              unless update_cache(event.path)
                @pending_updates[path] = nil
                @zk.defer { process_pending_updates }
              end
            else
              warn("session event=#{event.inspect}")
            end
          end
          @pending_updates[path] = nil unless update_cache(path)
        end
        @session_id = @zk.session_id
        @latch.count_down
      end

      @zk.on_expired_session do |e|
        info("on_expired_session #{e.event_name} #{e.state_name}")
      end

      @zk.on_exception do |e|
        error("on_exception exception=#{e.inspect} backtrace=#{e.backtrace.inspect}")
        ActiveSupport::Notifications.instrument(AS_NOTIFICATION_ERROR, error: e)
      end
    end

    def wait_for_warm_cache(timeout = 30)
      if @latch.wait(timeout)
        true
      else
        warn("didn't connect before timeout connected=#{@zk.connected?} timeout=#{timeout}")
        false
      end
    end

    def close!
      @watches.each_value(&:unsubscribe)
      @watches.clear
      @zk.close! if @owned_zk
    end

    def fetch(path)
      @cache.fetch(path).value
    end
    alias_method :[], :fetch

    def fetch_existing(path)
      cached = @cache.fetch(path)
      cached.value if cached.stat&.exists?
    end

    private

    def connect(host, zk_opts)
      raise Error, "already connected" if @zk

      debug("connecting host=#{host.inspect}")
      ZK.new(host, **zk_opts) do |zk|
        setup_callbacks(zk)
      end
    end

    # only called from ZK thread
    def update_cache(path)
      debug("update_cache path=#{path}")

      stat = @zk.stat(path, watch: true)

      instrument_name = AS_NOTIFICATION_UPDATE
      instrument_params = { path: path }

      unless stat.exists?
        value = @registered_values.fetch(path).default_value
        @cache[path] = CachedPath.new(value, stat)
        debug("no node, setting watch path=#{path}")
        instrument_params[:value] = value
        ActiveSupport::Notifications.instrument(instrument_name, instrument_params)
        return true
      end

      raw_value, stat = @zk.get(path, watch: true)

      instrument_params.merge!(
        latency_seconds: Time.now - stat.mtime_t,
        version: stat.version,
        data_length: stat.data_length,
      )

      value = begin
        registered_value = @registered_values.fetch(path)
        instrument_params[:value] = registered_value.deserialize(raw_value)
      rescue => e
        error(
          "deserialization error raw_zookeeper_value=#{raw_value.inspect} zookeeper_stat=#{stat.inspect} "\
          "exception=#{e.inspect} #{e.backtrace.inspect}"
        )
        instrument_name = AS_NOTIFICATION_ERROR
        instrument_params[:error] = e
        instrument_params[:raw_value] = raw_value
        registered_value.default_value
      end

      @cache[path] = CachedPath.new(value, stat)

      debug(
        "updated cache path=#{path} raw_value=#{raw_value.inspect} "\
        "value=#{value.inspect} stat=#{stat.inspect}"
      )
      ActiveSupport::Notifications.instrument(instrument_name, instrument_params)
      true
    rescue ::ZK::Exceptions::ZKError => e
      warn("update_cache path=#{path} exception=#{e.inspect}, retrying")
      retry
    rescue ::ZK::Exceptions::KeeperException, ::Zookeeper::Exceptions::ZookeeperException => e
      error("update_cache path=#{path} exception=#{e.inspect}")
      false
    end

    def process_pending_updates
      info("processing pending updates=#{@pending_updates.size}")
      @pending_updates.reject! do |missed_path, _|
        debug("update_cache with previously missed update path=#{missed_path}")
        update_cache(missed_path)
      end
      info("pending updates not processed=#{@pending_updates.size}")
    end

    %w(debug info warn error).each do |m|
      module_eval <<~EOM, __FILE__, __LINE__
        def #{m}(msg)
          return unless @logger
          @logger.#{m}("ZkRecipes::Cache") { msg }
        end
      EOM
    end

    CachedPath = Struct.new(:value, :stat)

    class RegisteredPath < Struct.new(:default_value, :deserializer)
      def deserialize(raw)
        deserializer ? deserializer.call(raw) : raw
      end
    end
  end
end
