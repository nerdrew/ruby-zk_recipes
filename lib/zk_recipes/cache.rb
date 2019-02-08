# frozen_string_literal: true

module ZkRecipes
  class Cache
    class Error < StandardError; end
    class PathError < Error; end
    class RuntimeRegistrationError < Error; end
    class StateError < Error; end

    AS_NOTIFICATION = "cache.zk_recipes"
    AS_NOTIFICATION_RUNTIME = "runtime_cache.zk_recipes"
    AS_NOTIFICATION_CHILDREN = "children_cache.zk_recipes"
    USE_DEFAULT = Object.new

    def initialize(logger: nil, host: nil, timeout: nil, zk_opts: {})
      @logger = logger
      @session_id = nil
      @zk = nil
      @latch = Concurrent::CountDownLatch.new

      # Concurrent::Hash preserves insertion order, but you can't mutate the hash while iterating
      @pending_updates = Concurrent::Map.new

      # These paths need to be registered *before* setup_callbacks
      @registered_paths = Concurrent::Map.new
      @registered_watches = Concurrent::Map.new

      # These paths need to be registered *before* setup_callbacks
      @registered_children_paths = Concurrent::Map.new
      @registered_children_watches = Concurrent::Map.new

      # These paths need to be registered *after* setup_callbacks
      @registered_runtime_paths = Concurrent::Map.new
      @registered_runtime_watches = Concurrent::Map.new

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

    # Can only be called before setup_callbacks
    # This method is not threadsafe. All configuration should be done on a single thread.
    def register(path, default_value, &deserializer)
      raise Error, "register only allowed before setup_callbacks called" if @zk
      raise Error, "path already registered path=#{path.inspect}" if @registered_paths.key?(path)

      @registered_paths[path] = RegisteredPath.new(default_value, deserializer)
      @logger&.debug { "added path=#{path} default_value=#{default_value.inspect}" }
      ActiveSupport::Notifications.instrument(AS_NOTIFICATION, path: path, value: default_value)
    end

    # Can only be called before setup_callbacks
    # This method is not threadsafe. All configuration should be done on a single thread.
    def register_children(path, default_children = [], &deserializer)
      raise Error, "register_children only allowed before setup_callbacks called" if @zk
      raise Error, "path already registered path=#{path.inspect}" if @registered_children_paths.key?(path)

      @registered_children_paths[path] = RegisteredPath.new(default_children, deserializer)
      @logger&.debug { "added children=#{path} default_children=#{default_children.inspect}" }
      ActiveSupport::Notifications.instrument(AS_NOTIFICATION_CHILDREN, path: path, value: default_children)
    end

    # Can only be called after setup_callbacks
    #
    # This method is threadsafe.
    # All accesses in this method need to be race-free.
    def register_runtime(path, default_value, &deserializer)
      # This check is threadsafe since @zk is only set once, (in `#setup_callbacks`). It is never changed after that.
      raise Error, "register_runtime only allowed after setup_callbacks called" unless @zk

      # This is not inherently threadsafe, but since you can't register non-runtime paths after `#setup_callbacks` is
      # called, and `#setup_callbacks` sets @zk, we know that `#setup_callbacks` has run, @zk is set, and no new paths
      # can be registered.
      raise RuntimeRegistrationError, "already registered path=#{path.inspect}" if registered_path?(path)

      already_registered = true

      # The registered_runtime_path needs to be set before the watch is registered, but they have two different locks.
      # Use the registered_runtime_watch as the authoritative lock so it can be held, set the registered_runtime_path,
      # then set the registered_runtime_watch. This means that to check if a runtime path is set, you must check if the
      # watch is set, not if the path is set.
      @registered_runtime_watches.compute_if_absent(path) do
        already_registered = false

        # The watch wasn't set, so the runtime path should *not* exist. If it does, it means there is a bug.
        if @registered_runtime_paths.put_if_absent(path, RegisteredPath.new(default_value, deserializer))
          @registered_runtime_paths.delete(path)
          raise StateError, "runtime path=#{path.inspect} should not exist if the watch is absent"
        end

        @zk.register(path) do |event|
          if event.node_event?
            @logger&.debug { "runtime node event path=#{event.path} #{event.event_name} #{event.state_name}" }
            unless update_registered_runtime_path(event.path)
              @pending_updates.put_if_absent(event.path, :runtime)
              @zk.defer(method(:process_pending_updates))
            end
          else
            @logger&.warn { "runtime session event #{event.event_name} #{event.state_name}" }
          end
        end
      end

      if already_registered
        raise RuntimeRegistrationError, "already registered runtime path=#{path.inspect}"
      end

      @zk.defer do
        @pending_updates.put_if_absent(path, :runtime)
        @zk.defer(method(:process_pending_updates))
      end
      @logger&.debug { "registered runtime path=#{path} default_value=#{default_value.inspect}" }
      ActiveSupport::Notifications.instrument(AS_NOTIFICATION_RUNTIME, path: path, value: default_value)
      nil
    end

    # Can only be called after setup_callbacks
    #
    # This method is threadsafe.
    # All accesses in this method need to be race-free.
    def unregister_runtime(path)
      raise Error, "register_runtime only allowed after setup_callbacks called" unless @zk

      not_registered = true

      # The runtime watch is the authoritative mutex for the runtime watch + the registered runtime path.
      # See `#register_runtime` for explanation.
      @registered_runtime_watches.compute_if_present(path) do |watch|
        not_registered = false
        watch.unregister
        @registered_runtime_paths.delete(path)

        # setting the value to nil will delete the key:
        # http://ruby-concurrency.github.io/concurrent-ruby/1.1.x/Concurrent/Map.html#compute_if_present-instance_method
        nil
      end

      raise PathError, "runtime path not registered path=#{path.inspect}" if not_registered

      @logger&.debug { "unregistered runtime path=#{path}" }
      nil
    end

    # All non-runtime registration must happen before setup_callbacks is called
    # This method is not threadsafe. All configuration should be done on a single thread.
    def setup_callbacks(zk_client)
      raise Error, "setup_callbacks can only be called once" if @zk

      @zk = zk_client

      if @zk.connected? || @zk.connecting?
        raise Error, "the ZK::Client is already connected, the cached values must be set before connecting"
      end

      @registered_paths.each do |path, _value|
        @registered_watches[path] = @zk.register(path) do |event|
          if event.node_event?
            @logger&.debug { "node event path=#{event.path} #{event.event_name} #{event.state_name}" }
            unless update_registered_path(event.path)
              @pending_updates.put_if_absent(event.path, :init)
              @zk.defer(method(:process_pending_updates))
            end
          else
            @logger&.warn { "session event #{event.event_name} #{event.state_name}" }
          end
        end
      end

      @registered_children_paths.each do |path, _child|
        @registered_children_watches[path] = @zk.register(path) do |event|
          if event.node_event?
            @logger&.debug { "child event path=#{event.path} #{event.event_name} #{event.state_name}" }
            unless update_registered_children_path(event.path)
              @pending_updates.put_if_absent(event.path, :children)
              @zk.defer(method(:process_pending_updates))
            end
          else
            @logger&.warn { "session event #{event.event_name} #{event.state_name}" }
          end
        end
      end

      @registered_watches["on_connected"] = @zk.on_connected do
        if @session_id == @zk.session_id
          @logger&.debug("on_connected: reconnected existing session")
          process_pending_updates
          next
        end

        @logger&.debug("on_connected: new session")
        @pending_updates.clear
        @registered_paths.each_key do |path|
          @pending_updates.put_if_absent(path, :init) unless update_registered_path(path)
        end
        @registered_children_paths.each_key do |path|
          @pending_updates.put_if_absent(path, :children) unless update_registered_children_path(path)
        end
        @registered_runtime_paths.each_key do |path|
          @pending_updates.put_if_absent(path, :runtime) unless update_registered_runtime_path(path)
        end
        @session_id = @zk.session_id
        @latch.count_down

        @zk.defer(method(:process_pending_updates))
      end

      @zk.on_exception do |e|
        @logger&.error { "on_exception: exception=#{e.inspect} backtrace=#{e.backtrace.inspect}" }
      end
    end

    # This method is not threadsafe. All configuration should be done on a single thread.
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
      [@registered_runtime_watches, @registered_watches, @registered_children_watches].each do |watches|
        watches.each_key do |path|
          watches.compute_if_present(path) do |watch|
            watch.unregister
            nil
          end
        end
      end
      @zk.close! if @owned_zk
      @pending_updates.clear
    end

    # reopen the client after the process forks
    # This is *not* the opposite of `#close!`.
    # This method is not threadsafe. All configuration should be done on a single thread.
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

    # This method is threadsafe.
    def registered_path?(path)
      @registered_paths.key?(path)
    end

    # This method is threadsafe.
    def registered_runtime_path?(path)
      # The runtime watch is the authoritative source for "this runtime path exists".
      # See `#register_runtime` for explanation.
      @registered_runtime_watches.key?(path)
    end

    # This method is threadsafe.
    def fetch(path)
      inner_fetch(path, @registered_paths)
    end
    alias_method :[], :fetch

    # This method is threadsafe.
    def fetch_valid(path)
      inner_fetch_valid(path, @registered_paths)
    end

    # This method is threadsafe.
    def fetch_runtime(path)
      inner_fetch(path, @registered_runtime_paths)
    end

    # This method is threadsafe.
    def fetch_runtime_valid(path)
      inner_fetch_valid(path, @registered_runtime_paths)
    end

    # This method is threadsafe.
    def fetch_children(path)
      inner_fetch(path, @registered_children_paths)
    end

    # This method is threadsafe.
    def fetch_children_valid(path)
      inner_fetch_valid(path, @registered_children_paths)
    end

    private

    def inner_fetch(path, registered_paths)
      registered_path = registered_paths[path]
      raise PathError, "no registered path for #{path.inspect}" unless registered_path

      registered_path.value
    end

    def inner_fetch_valid(path, registered_paths)
      registered_path = registered_paths[path]
      raise PathError, "no registered path for #{path.inspect}" unless registered_path

      registered_path.value if registered_path.valid?
    end

    # Only called for owned ZK connections.
    def connect(host, zk_opts)
      raise Error, "already connected" if @zk

      @logger&.debug { "connecting host=#{host.inspect}" }
      ZK.new(host, **zk_opts) do |zk|
        setup_callbacks(zk)
      end
    end

    def update_registered_path(path)
      update_node(path, @registered_paths, AS_NOTIFICATION)
    end

    def update_registered_runtime_path(path)
      return true unless @registered_runtime_paths.key?(path)

      update_node(path, @registered_runtime_paths, AS_NOTIFICATION_RUNTIME)
    rescue KeyError
      @logger&.warn { "attempted to update runtime cache for non-existent path=#{path}" }
      return true
    end

    def update_registered_children_path(path)
      update_node(path, @registered_children_paths, AS_NOTIFICATION_CHILDREN, :children)
    end

    def update_node(path, registered_paths, notification, zk_method = :get)
      unless @zk.connected?
        @logger&.debug { "update_node: zk not connected, skipping update path=#{path}" }
        return false
      end

      raise StateError, "update_node can only be called from the ZK thread" unless @zk.on_threadpool?

      stat = @zk.stat(path, watch: true)

      registered_path = registered_paths.fetch(path)
      instrument_params = { path: path, old_value: registered_path.value }

      unless stat.exists?
        registered_path.reset_value!(stat)
        @logger&.debug { "update_node: no data node, setting watch path=#{path}" }
        instrument_params[:value] = registered_path.value
        ActiveSupport::Notifications.instrument(notification, instrument_params)
        return true
      end

      raw_value = @zk.public_send(zk_method, path, watch: true)
      if raw_value.last.is_a?(::ZK::Stat)
        stat = raw_value.pop
        raw_value = raw_value.pop
      end

      instrument_params[:latency_seconds] = Time.now - stat.mtime_t
      instrument_params[:version] = stat.version
      instrument_params[:children_version] = stat.child_list_version
      instrument_params[:data_length] = stat.data_length

      begin
        instrument_params[:value] = registered_path.deserialize(raw_value, stat)
      rescue => e
        @logger&.error { "deserialization error path=#{path} stat=#{stat.inspect} exception=#{e.inspect} #{e.backtrace.inspect}" }
        instrument_params[:error] = e
        instrument_params[:raw_value] = raw_value
      end

      ActiveSupport::Notifications.instrument(notification, instrument_params)
      @logger&.debug { "update_node: path=#{path} raw_value=#{raw_value.inspect} value=#{registered_path.value.inspect} stat=#{stat.inspect}" }
      true
    rescue ::ZK::Exceptions::ZKError => e
      @logger&.warn { "update_node: path=#{path} exception=#{e.inspect}, retrying" }
      retry
    rescue ::ZK::Exceptions::KeeperException, ::Zookeeper::Exceptions::ZookeeperException => e
      @logger&.error { "update_node: path=#{path} exception=#{e.inspect}" }
      false
    end

    # TODO: add backoff when errors loop?
    def process_pending_updates
      if @pending_updates.empty?
        @logger&.debug("process_pending_updates: no pending updates")
        return
      end

      unless @zk.connected?
        @logger&.debug("process_pending_updates: zk not connected")
        return
      end

      @logger&.debug { "process_pending_updates: updates=#{@pending_updates.size}" }
      @pending_updates.each do |path, type|
        @logger&.debug { "process_pending_updates: path=#{path} type=#{type}" }
        delete = case type
                 when :init
                   update_registered_path(path)
                 when :runtime
                   update_registered_runtime_path(path)
                 when :children
                   update_registered_children_path(path)
                 else
                   @logger&.error { "process_pending_updates: unknown pending_update type=#{type.inspect}" }
                   true
                 end
        @pending_updates.delete(path) if delete
      end

      if @pending_updates.empty?
        @logger&.debug("process_pending_updates: processed all pending updates")
      else
        @logger&.warn { "process_pending_updates: failed to process pending updates=#{@pending_updates.inspect}" }
      end
    end

    class RegisteredPath
      attr_reader :value, :stat, :default_value

      def initialize(default_value, deserializer)
        @value = @default_value = default_value
        @deserializer = deserializer
        @stat = nil
        @valid = false
      end

      def valid?
        @valid
      end

      def reset_value!(stat = nil)
        @value = @default_value
        @stat = stat if stat
      end

      def deserialize(raw, stat = nil)
        @stat = stat if stat
        @value = @deserializer ? @deserializer.call(raw) : raw
        @valid = true
        if @value == USE_DEFAULT
          @value = @default_value
          @valid = false
        end
        @value
      rescue
        @value = @default_value
        @valid = false
        raise
      end
    end
    private_constant :RegisteredPath
  end
end
