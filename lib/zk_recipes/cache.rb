# frozen_string_literal: true

module ZkRecipes
  class Cache
    class Error < StandardError; end
    class PathError < Error; end
    class StateError < Error; end

    AS_NOTIFICATION = "cache.zk_recipes"
    AS_NOTIFICATION_DIRECTORY = "directory_cache.zk_recipes"
    USE_DEFAULT = Object.new

    STATIC = Object.new
    private_constant :STATIC

    def initialize(logger: nil, host: nil, timeout: nil, zk_opts: {})
      @logger = logger
      @session_id = nil
      @zk = nil
      @latch = Concurrent::CountDownLatch.new

      # Concurrent::Hash preserves insertion order, but you can't mutate the hash while iterating
      @pending_updates = Concurrent::Map.new

      # These paths need to be registered *before* setup_callbacks
      @static_paths = {}
      @static_watches = Concurrent::Map.new

      # These paths need to be registered *before* setup_callbacks
      @directory_paths = {}
      @directory_watches = Concurrent::Map.new

      # These paths need to be registered *after* setup_callbacks
      @runtime_paths = Concurrent::Map.new
      @runtime_watches = Concurrent::Map.new

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

    # Register a path to watch and cache values.
    #
    # Can only be called before setup_callbacks
    # This method is not threadsafe. All configuration should be done on a single thread.
    #
    # @param path [String]
    #   Path to watch.
    #
    # @param default_value [Object]
    #   Value when the path is not set or there was a deserialization error.
    #
    # @param deserializer [block]
    #   Callable that takes a string and returns the cached value for the path.
    #
    # e.g.:
    #   cache.register("/test/foo", 1) { |raw_value| raw_value.to_i * 2 }
    def register(path, default_value, &deserializer)
      raise Error, "register only allowed before setup_callbacks called" if @zk
      raise Error, "path already registered path=#{path.inspect}" if @static_paths.key?(path)

      @static_paths[path] = StaticPath.new(default_value, deserializer)
      @logger&.debug { "added path=#{path} default_value=#{default_value.inspect}" }
      ActiveSupport::Notifications.instrument(AS_NOTIFICATION, path: path, value: default_value)
    end

    # Register a directory to watch. Children are mapped to paths that are also watched and their values cached.
    #
    # Can only be called before setup_callbacks
    # This method is not threadsafe. All configuration should be done on a single thread.
    #
    # @param path [String]
    #   Path to watch.
    #
    # @param path_mapper [Callable<String => Object>]
    #   Takes a child of the watched directory and returns path to watch.
    #
    # @param deserializer [block<String => Object>]
    #   Takes a string and returns the cached value for the path.
    #
    # e.g.:
    #   cache.register_directory("/groups/groupA", ->(child) { "/test/#{child}" }) do |raw_value|
    #     "#{raw_value}!"
    #   end
    #
    # Sets up a watch on /groups/groupA. Children of groupA are mapped to /test/<child> (/groups/groupA/boom => /test/boom).
    # Watches are set on the mapped children and parsed using the deserializer. If the mapped path was registered with
    # `#register`, that cached value is used instead of this deserializer.
    def register_directory(path, path_mapper = ->(pth) { pth }, &deserializer)
      raise Error, "register_directory only allowed before setup_callbacks called" if @zk
      raise Error, "path already registered path=#{path.inspect}" if @directory_paths.key?(path)

      @directory_paths[path] = Directory.new(path_mapper, deserializer)
      @logger&.debug { "added directory=#{path}" }
      ActiveSupport::Notifications.instrument(AS_NOTIFICATION_DIRECTORY, path: path, directory_paths: [])
    end

    # All non-runtime registration must happen before setup_callbacks is called
    # This method is not threadsafe. All configuration should be done on a single thread.
    def setup_callbacks(zk_client)
      raise Error, "setup_callbacks can only be called once" if @zk

      @zk = zk_client

      if @zk.connected? || @zk.connecting?
        raise Error, "the ZK::Client is already connected, the cached values must be set before connecting"
      end

      @static_paths.each do |path, _value|
        @static_watches[path] = @zk.register(path) do |event|
          if event.node_event?
            @logger&.debug { "node event path=#{event.path} #{event.event_name} #{event.state_name}" }
            unless update_static_path(event.path)
              @pending_updates.put_if_absent(event.path, :static)
              @zk.defer(method(:process_pending_updates))
            end
          else
            @logger&.warn { "session event #{event.event_name} #{event.state_name}" }
          end
        end
      end

      @directory_paths.each do |path, _child|
        @directory_watches[path] = @zk.register(path) do |event|
          if event.node_event?
            @logger&.debug { "child event path=#{event.path} #{event.event_name} #{event.state_name}" }
            unless update_directory_path(event.path)
              @pending_updates.put_if_absent(event.path, :directory)
              @zk.defer(method(:process_pending_updates))
            end
          else
            @logger&.warn { "session event #{event.event_name} #{event.state_name}" }
          end
        end
      end

      @static_watches["on_connected"] = @zk.on_connected do
        if @session_id == @zk.session_id
          @logger&.debug("on_connected: reconnected existing session")
          process_pending_updates
          next
        end

        @logger&.debug("on_connected: new session")
        @pending_updates.clear
        @static_paths.each_key do |path|
          @pending_updates.put_if_absent(path, :static) unless update_static_path(path)
        end
        @directory_paths.each_key do |path|
          @pending_updates.put_if_absent(path, :directory) unless update_directory_path(path)
        end
        @runtime_paths.each_key do |path|
          @pending_updates.put_if_absent(path, :runtime) unless update_runtime_path(path)
        end
        @session_id = @zk.session_id
        @latch.count_down

        @zk.defer(method(:process_pending_updates))
      end

      @zk.on_exception do |e|
        @logger&.error { "on_exception: exception=#{e.inspect} backtrace=#{e.backtrace.inspect}" }
      end

      @static_paths.freeze
      @directory_paths.freeze
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

    # TODO should this poison the whole cache?
    def close!
      [@runtime_watches, @static_watches, @directory_watches].each do |watches|
        watches.each_key do |path|
          watches.compute_if_present(path) do |watch|
            watch.unregister
            nil
          end
        end
      end
      @zk.close! if @owned_zk
      @pending_updates.clear
      @runtime_paths.clear
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

    # Fetch the parsed ZK value or default value if the ZK value is missing or corrupt.
    #
    # This method is threadsafe.
    #
    # @param path [String]
    #
    # @return [Object]
    #   The parsed value for `path` or the default value if `path` is not set or there was a deserialization error.
    def fetch(path)
      static_path = @static_paths[path]
      raise PathError, "no registered path for #{path.inspect}" unless static_path

      static_path.value
    end
    alias_method :[], :fetch

    # Fetch valid parsed ZK value only, do not return the default value if the ZK value is missing or corrupt.
    #
    # This method is threadsafe.
    #
    # @param path [String]
    #
    # @return [Object]
    #   The parsed value for `path` or `nil` if `path` is not set or there was a deserialization error.
    def fetch_valid(path)
      static_path = @static_paths[path]
      raise PathError, "no registered path for #{path.inspect}" unless static_path

      static_path.valid_value
    end

    # Fetch the values for each mapped child of `path`.
    #
    # This method is threadsafe.
    #
    # @param path [String]
    #   The directory path.
    #
    # Mapped paths that:
    # - don't exist and aren't static registered paths (see `#register`) are not included.
    # - do exist and aren't static registered paths but had deserialization errors are not included.
    # - are static registered paths are included and will be the parsed or default value.
    #
    # @note: if multiple registered directories have children that map to the same path and that path is not statically
    # registered, each directory's deserializer will be used to parse that path's value.
    #
    # Example: {file:examples/cache.rb}.
    #
    # @return [Hash<String, Object>]
    #   Hash of parsed values for each child's mapped path in the directory.
    def fetch_directory_values(path)
      directory = @directory_paths[path]
      raise PathError, "no registered path for #{path.inspect}" unless directory

      values = directory.values
      values.each do |child_path, value|
        next unless value == STATIC

        values[child_path] = @static_paths[child_path].value
      end
      values
    end

    private

    # Only called for owned ZK connections.
    def connect(host, zk_opts)
      raise Error, "already connected" if @zk

      @logger&.debug { "connecting host=#{host.inspect}" }
      ZK.new(host, **zk_opts) do |zk|
        setup_callbacks(zk)
      end
    end

    # This method is threadsafe.
    # All accesses in this method need to be race-free.
    def register_runtime(path, directory)
      # This check is threadsafe since @zk is only set once, (in `#setup_callbacks`). It is never changed after that.
      raise Error, "register_runtime only allowed after setup_callbacks called" unless @zk

      # The runtime_path needs to be set before the watch is registered, but they have two different locks.
      # Use the runtime_watch as the authoritative lock so it can be held, set the runtime_path,
      # then set the runtime_watch. This means that to check if a runtime path is set, you must check if the
      # watch is set, not if the path is set.
      @runtime_watches.compute(path) do |watch|
        # This is not inherently threadsafe, but since you can't register non-runtime paths after `#setup_callbacks` is
        # called, and `#setup_callbacks` sets @zk, we know that `#setup_callbacks` has run, @zk is set, and no new paths
        # can be registered.
        @runtime_paths.compute(path) do |directories|
          directories ||= Set.new.compare_by_identity
          directories << directory
          directories
        end

        next watch if watch

        @zk.register(path) do |event|
          if event.node_event?
            @logger&.debug { "runtime node event path=#{event.path} #{event.event_name} #{event.state_name}" }
            unless update_runtime_path(event.path)
              @pending_updates.put_if_absent(event.path, :runtime)
              @zk.defer(method(:process_pending_updates))
            end
          else
            @logger&.warn { "runtime session event #{event.event_name} #{event.state_name}" }
          end
        end
      end

      @zk.defer do
        @pending_updates.put_if_absent(path, :runtime)
        @zk.defer(method(:process_pending_updates))
      end
      nil
    end

    # Can only be called after setup_callbacks
    #
    # This method is threadsafe.
    # All accesses in this method need to be race-free.
    def unregister_runtime(path, directory)
      raise Error, "register_runtime only allowed after setup_callbacks called" unless @zk

      not_registered = true

      # The runtime watch is the authoritative mutex for the runtime watch + the registered runtime path.
      # See `#register_runtime` for explanation.
      @runtime_watches.compute_if_present(path) do |watch|
        @runtime_paths.compute_if_present(path) do |directories|
          not_registered = false

          directories.delete(directory)
          if directories.empty?
            @logger&.debug { "unregistered runtime watch path=#{path}" }
            watch.unregister
            watch = nil
            nil
          else
            directories
          end
        end

        # setting the value to nil will delete the key:
        # http://ruby-concurrency.github.io/concurrent-ruby/1.1.x/Concurrent/Map.html#compute_if_present-instance_method
        watch
      end

      @logger&.error { "runtime path not registered=#{path.inspect}" } if not_registered
    end

    def update_static_path(path)
      unless @zk.connected?
        @logger&.debug { "update_static_path: zk not connected, skipping update path=#{path}" }
        return false
      end

      raise StateError, "update_static_path can only be called from the ZK thread" unless @zk.on_threadpool?

      stat = @zk.stat(path, watch: true)

      static_path = @static_paths.fetch(path)
      instrument_params = { path: path }

      unless stat.exists?
        static_path.reset_value!(stat)
        @logger&.debug { "update_static_path: no data node, setting watch path=#{path}" }
        instrument_params[:value] = static_path.value
        ActiveSupport::Notifications.instrument(AS_NOTIFICATION, instrument_params)
        return true
      end

      raw_value, stat = @zk.get(path, watch: true)

      instrument_params[:latency_seconds] = Time.now - stat.mtime_t
      instrument_params[:version] = stat.version
      instrument_params[:directory_version] = stat.child_list_version
      instrument_params[:data_length] = stat.data_length

      begin
        instrument_params[:value] = static_path.deserialize(raw_value, stat)
      rescue => e
        @logger&.error { "deserialization error path=#{path} stat=#{stat.inspect} exception=#{e.inspect} #{e.backtrace.inspect}" }
        instrument_params[:error] = e
        instrument_params[:raw_value] = raw_value
      end

      ActiveSupport::Notifications.instrument(AS_NOTIFICATION, instrument_params)
      @logger&.debug { "update_static_path: path=#{path} raw_value=#{raw_value.inspect} value=#{static_path.value.inspect} stat=#{stat.inspect}" }
      true
    rescue ::ZK::Exceptions::ZKError => e
      @logger&.warn { "update_static_path: path=#{path} exception=#{e.inspect}, retrying" }
      retry
    rescue ::ZK::Exceptions::KeeperException, ::Zookeeper::Exceptions::ZookeeperException => e
      @logger&.error { "update_static_path: path=#{path} exception=#{e.inspect}" }
      false
    end

    def update_runtime_path(path)
      unless @zk.connected?
        @logger&.debug { "update_runtime_path: zk not connected, skipping update path=#{path}" }
        return false
      end

      raise StateError, "update_runtime_path: can only be called from the ZK thread" unless @zk.on_threadpool?

      stat = @zk.stat(path, watch: true)

      directories = @runtime_paths.fetch(path)

      unless stat.exists?
        directories.each { |directory| directory.remove_path!(path) }
        @logger&.debug { "update_runtime_path: no data node, setting watch path=#{path}" }
        return true
      end

      raw_value, stat = @zk.get(path, watch: true)

      directories.each do |directory|
        if (e = directory.update_path!(path, raw_value))
          @logger&.error { "deserialization error path=#{path} stat=#{stat.inspect} exception=#{e.inspect} #{e.backtrace.inspect}" }
        end
      end

      @logger&.debug { "update_runtime_path: path=#{path} raw_value=#{raw_value.inspect} stat=#{stat.inspect}" }
      true
    rescue ::ZK::Exceptions::ZKError => e
      @logger&.warn { "update_runtime_path: path=#{path} exception=#{e.inspect}, retrying" }
      retry
    rescue ::ZK::Exceptions::KeeperException, ::Zookeeper::Exceptions::ZookeeperException => e
      @logger&.error { "update_runtime_path: path=#{path} exception=#{e.inspect}" }
      false
    end

    def update_directory_path(path)
      unless @zk.connected?
        @logger&.debug { "update_directory_path: zk not connected, skipping update path=#{path}" }
        return false
      end

      raise StateError, "update_directory_path: can only be called from the ZK thread" unless @zk.on_threadpool?

      stat = @zk.stat(path, watch: true)

      directory = @directory_paths.fetch(path)
      instrument_params = { path: path }

      unless stat.exists?
        instrument_params[:directory_paths] = []
        directory.reset_paths!(stat).each do |removed_path|
          unregister_runtime(removed_path, directory) unless @static_paths.key?(removed_path)
        end
        @logger&.debug { "update_directory_path: no node, setting watch path=#{path}" }
        ActiveSupport::Notifications.instrument(AS_NOTIFICATION_DIRECTORY, instrument_params)
        return true
      end

      raw_child_paths = @zk.children(path, watch: true)

      instrument_params[:latency_seconds] = Time.now - stat.mtime_t
      instrument_params[:version] = stat.version
      instrument_params[:directory_version] = stat.child_list_version
      instrument_params[:data_length] = stat.data_length

      new_paths = nil
      begin
        new_paths, removed_paths = directory.map_paths(raw_child_paths, stat)
        new_paths.each do |new_path|
          if @static_paths.key?(new_path)
            directory.static_path!(new_path)
            @logger&.debug { "update_directory_path: path=#{path} has static child=#{new_path}" }
          else
            register_runtime(new_path, directory)
            @logger&.debug { "update_directory_path: path=#{path} has runtime child=#{new_path}" }
          end
        end
        removed_paths.each do |removed_path|
          unregister_runtime(removed_path, directory) unless @static_paths.key?(removed_path)
          @logger&.debug { "update_directory_path: path=#{path} removed child=#{removed_path}" }
        end
        instrument_params[:directory_paths] = directory.paths
      rescue => e
        @logger&.error { "map_path error path=#{path} stat=#{stat.inspect} exception=#{e.inspect} #{e.backtrace.inspect}" }
        instrument_params[:error] = e
        instrument_params[:raw_child_paths] = raw_child_paths
      end

      ActiveSupport::Notifications.instrument(AS_NOTIFICATION_DIRECTORY, instrument_params)
      @logger&.debug { "update_directory_path: path=#{path} raw_child_paths=#{raw_child_paths.inspect} paths=#{new_paths} stat=#{stat.inspect}" }
      true
    rescue ::ZK::Exceptions::ZKError => e
      @logger&.warn { "update_directory_path: path=#{path} exception=#{e.inspect}, retrying" }
      retry
    rescue ::ZK::Exceptions::KeeperException, ::Zookeeper::Exceptions::ZookeeperException => e
      @logger&.error { "update_directory_path: path=#{path} exception=#{e.inspect}" }
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
        delete =
          case type
          when :static
            update_static_path(path)
          when :runtime
            update_runtime_path(path)
          when :directory
            update_directory_path(path)
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

    class StaticPath
      def initialize(default_value, deserializer)
        @mutex = Mutex.new
        @value = @default_value = default_value
        @deserializer = deserializer
        @stat = nil
        @valid = false
      end

      def value
        @value
      end

      def valid_value
        @mutex.synchronize { @value if @valid }
      end

      def reset_value!(stat)
        @mutex.synchronize do
          @value = @default_value
          @valid = false
        end
        @stat = stat
      end

      def deserialize(raw, stat = nil)
        @stat = stat if stat
        value = @deserializer ? @deserializer.call(raw) : raw
        valid = true
        if value == USE_DEFAULT
          value = @default_value
          valid = false
        end
        @mutex.synchronize do
          @valid = valid
          @value = value
          @value
        end
      rescue
        @mutex.synchronize do
          @value = @default_value
          @valid = false
        end
        raise
      end
    end
    private_constant :StaticPath

    class Directory
      def initialize(path_mapper, deserializer)
        @mutex = Mutex.new
        @path_mapper = path_mapper
        @deserializer = deserializer

        @watched_paths = Set.new
        @paths_with_values = {}
        @stat = nil
      end

      def reset_paths!(stat)
        paths = @watched_paths.to_a
        @mutex.synchronize do
          @watched_paths.clear
          @paths_with_values.clear
        end
        @stat = stat
        paths
      end

      def map_paths(paths, stat)
        @stat = stat
        removed_paths = nil
        added_paths = nil
        incoming_paths = Set.new(paths.map { |path| @path_mapper.call(path) })

        @mutex.synchronize do
          removed_paths = @watched_paths.difference(incoming_paths)
          added_paths = incoming_paths.difference(@watched_paths)

          @paths_with_values.delete_if { |k, _v| !incoming_paths.include?(k) }
          @watched_paths = incoming_paths
        end
        [added_paths, removed_paths]
      end

      def paths
        @mutex.synchronize { @watched_paths.to_a }
      end

      def values
        @mutex.synchronize { @paths_with_values.dup }
      end

      def static_path!(path)
        @mutex.synchronize { @paths_with_values[path] = STATIC }
      end

      def remove_path!(path)
        @mutex.synchronize { @paths_with_values.delete(path) }
      end

      def update_path!(path, raw_value)
        value = @deserializer ? @deserializer.call(raw_value) : raw_value
        if USE_DEFAULT == value
          @mutex.synchronize { @paths_with_values.delete(path) }
        else
          @mutex.synchronize { @paths_with_values[path] = value }
        end
        nil
      rescue => e
        @mutex.synchronize { @paths_with_values.delete(path) }
        e
      end
    end
    private_constant :Directory
  end
end
