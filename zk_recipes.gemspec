# coding: utf-8

lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'zk_recipes/version'

Gem::Specification.new do |spec|
  spec.name = "zk_recipes"
  spec.version = ZkRecipes::VERSION
  spec.authors = ["Andrew Lazarus"]
  spec.email = ["nerdrew@gmail.com"]

  spec.summary = %q{Common Recipes for Zookeeper}
  spec.homepage = "https://github.com/nerdrew/zk_recipes"
  spec.license = "MIT"

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  if spec.respond_to?(:metadata)
    spec.metadata['allowed_push_host'] = "TODO: Set to 'http://mygemserver.com'"
  else
    raise "RubyGems 2.0 or newer is required to protect against " \
      "public gem pushes."
  end

  spec.files = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency "activesupport", ">= 4.0"
  spec.add_runtime_dependency "concurrent-ruby", "~> 1.0"
  spec.add_runtime_dependency "zk", "~> 1.9"
end
