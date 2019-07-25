# ZkRecipes

## Installation

Add this line to your application's Gemfile:

```ruby
gem "zk_recipes"
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install zk_recipes

## Usage

### A cache that creates its own ZK client:

```ruby
logger = Logger.new(STDERR)
cache = ZkRecipes::Cache.new(host: "my-host:1234", logger: logger, timeout: 10, zk_opts: { timeout: 5 }) do |z|
  z.register("/test/boom", "goat")
  z.register("/test/foo", 1) { |raw_value| raw_value.to_i * 2 }
  z.register_directory("/test/groupA", ->(child) { "/test/#{child}" }) do |raw_value|
    "#{raw_value}s!"
  end
end

puts cache["/test/boom"] # => "goat"

zk.mkdir_p("/test/groupA/foo")
zk.create("/test/foo", "3")
puts cache.fetch_directory_values("/test/groupA") # => { "/test/foo" => 6 }

cache.close!
```

### A cache that uses an existing ZK client:

```ruby
logger = Logger.new(STDERR)
zk = ZK.new("my-host:1234", connect: false)
cache = ZkRecipes::Cache.new(logger: logger)
cache.register("/test/boom", "goat") { |string| "Hello, #{string}" }
cache.setup_callbacks(zk) # no more paths can be registered after this

puts cache["/test/boom"] # => "Hello, goat"

zk.connect
cache.wait_for_warm_cache(10) # wait up to 10s for the cache to warm

zk.create("/test/boom")
zk.set("/test/boom", "cat")

sleep 1

puts cache["/test/boom"] # => "Hello, cat"
cache.close!
zk.close!
```

### Handling forks with a cache that creates it's own ZK client:

```ruby
zk = ZK.new("my-host:1234") # zk client for writing only

logger = Logger.new(STDERR)
cache = ZkRecipes::Cache.new(host: "my-host:1234", logger: logger, timeout: 10) do |z|
  z.register("/test/boom", "goat")
end


puts cache["/test/boom"] # => "goat"

if fork
  # parent
  zk.set("/test/boom", "mouse")
  cache.close!
  zk.close!
else
  # child
  cache.reopen # wait up to 10s for ZK to reconnect and the cache to warm
  puts cache["/test/boom"] # => "mouse"
  cache.close!
  zk.close!
end
```

### Handling forks with an existing ZK client:

```ruby
logger = Logger.new(STDERR)
zk = ZK.new("my-host:1234", connect: false)
cache = ZkRecipes::Cache.new(logger: logger)
cache.register("/test/boom", "goat")
cache.setup_callbacks(zk) # no more paths can be registered after this

puts cache["/test/boom"] # => "goat"

zk.connect
cache.wait_for_warm_cache(10) # wait up to 10s for the cache to warm

zk.create("/test/boom")
zk.set("/test/boom", "cat")

sleep 1

puts cache["/test/boom"] # => "cat"

if fork
  # parent
  zk.set("/test/boom", "mouse")
  cache.close!
  zk.close!
else
  # child
  cache.reopen
  zk.reopen
  cache.wait_for_warm_cache(10) # wait up to 10s for the cache to warm again
  puts cache["/test/boom"] # => "mouse"
  cache.close!
  zk.close!
end
```

### ActiveSupport::Notifications

`ZkRecipes` use `ActiveSupport::Notifications` for callbacks.

WARNING: exceptions raised in `ActiveSupport::Notifications.subscribe` will
bubble up to the line where the notification is instrumented. MAKE SURE YOU
CATCH EXCEPTIONS IN YOUR SUBSCRIBE BLOCKS!

Example:

```ruby
ActiveSupport::Notifications.subscribe("cache.zk_recipes") do |_name, _start, _finish, _id, payload|
  if payload[:error]
    puts "There was a ZkRecipes::Cache error #{payload[:error]}"
    puts "The error occurred trying to deserialize a value from ZK "\
      "path=#{payload[:path] raw_zk_value=#{payload[:raw_value]}"
  else
    puts "Received and update for path=#{payload[:path}."
    puts "The update in ZK happened #{payload[:latency_seconds]} seconds ago."
    puts "The path's data is #{payload[:data_length]} bytes."
    puts "The path's version is #{payload[:version]}."
  end
end
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run
`rake spec` to run the tests. You can also run `bin/console` for an interactive
prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To
release a new version, update the version number in `version.rb`, and then run
`bundle exec rake release`, which will create a git tag for the version, push
git commits and tags, and push the `.gem` file to
[rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at
https://github.com/nerdrew/ruby-zk_recipes. This project is intended to be a
safe, welcoming space for collaboration, and contributors are expected to
adhere to the [Contributor Covenant](http://contributor-covenant.org) code of
conduct.

## License

The gem is available as open source under the terms of the [MIT
License](http://opensource.org/licenses/MIT).
