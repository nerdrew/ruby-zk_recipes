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
  spec.homepage = "https://github.com/nerdrew/ruby-zk_recipes"
  spec.license = "MIT"

  spec.files = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(bin|test|spec|features)/})
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  unless RUBY_ENGINE == "jruby"
    spec.required_ruby_version = '>= 2.4'
  end

  spec.add_runtime_dependency "activesupport", ">= 4.0"
  spec.add_runtime_dependency "concurrent-ruby", "~> 1.0"
  spec.add_runtime_dependency "zk", "~> 1.9"
end
