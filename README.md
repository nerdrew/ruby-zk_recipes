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
end

puts cache["/test/boom"]
cache.close!
```

### A cache that uses an existing ZK client:

```ruby
logger = Logger.new(STDERR)
zk = ZK.new("my-host:1234", connect: false, timeout: 5) # ZK timeout = 5s
cache = ZkRecipes::Cache.new(logger: logger)
cache.register("/test/boom", "goat")
cache.register("/test/foo", 1) { |raw_value| raw_value.to_i * 2 }
cache.setup_callbacks(zk) # no more paths can be registered after this

puts cache["/test/boom"] # => "goat"

zk.connect
cache.wait_for_warm_cache(10) # wait 10s for the cache to warm

zk.create("/test/boom")
zk.set("/test/boom", "cat")

sleep 1

puts cache["/test/boom"] # => "cat"
cache.close!
zk.close!
```

### ActiveSupport::Notifications

`ZkRecipes` use `ActiveSupport::Notifications` for callbacks.
`ZkRecipes::Cache` has two different notifications: `zk_recipes.cache.update`
and `zk_recipes.cache.error`.

WARNING: exceptions raised in `ActiveSupport::Notifications.subscribe` will
bubble up to the line where the notification is instrumented. MAKE SURE YOU
CATCH YOUR EXCEPTIONS IN YOUR SUBSCRIBE BLOCKS!

Example:

```ruby
ActiveSupport::Notifications.subscribe("zk_recipes.cache.update") do |_name, _start, _finish, _id, payload|
  begin
    puts "Received and update for path=#{payload[:path}."
    puts "The update in ZK happened #{payload[:latency_seconds]} seconds ago."
    puts "The path's data is #{payload[:data_length]} bytes."
    puts "The path's version is #{payload[:version]}."
  rescue => e
    warn "there was an error=#{e.inspect}"
  end
end

ActiveSupport::Notifications.subscribe("zk_recipes.cache.error") do |_name, _start, _finish, _id, payload|
  begin
    puts "There was a ZkRecipes::Cache error #{payload[:error]}"
    if payload[:path]
      puts "The error occurred trying to deserialize a value from ZK "\
        "path=#{payload[:path] raw_zk_value=#{payload[:raw_value]}"
    end
  rescue => e
    warn "there was an error=#{e.inspect}"
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

