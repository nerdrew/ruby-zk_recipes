# Changelog

## 0.2.3

- `ZkRecipes::Cache#close!` should not cause exceptions if there are zk events in the zk queue

## 0.2.2

- More logging tweaks: use block form of logging for everything *execpt* string literals
- Add `ZkRecipes::Cache::USE_DEFAULT` marker so deserializers can return a default value without raising

## 0.2.1

- Tweak logging

## 0.2.0

This version includes BREAKING CHANGES. See below

- BREAKING CHANGE: rename fetch_existing to fetch_valid, it checks the path
  exists AND the value successfully deserialized.
- cleanup logging
- Make `on_connected` lighter: `on_connected` gets called for every watch when
  a connection flaps. Make the happy path `on_connected` faster.
- Add `ZkRecipes::Cache#reopen` for resetting the cache after a `fork`
- BREAKING CHANGE: Don't use `KeyError` as part of the API; use
  `ZkRecipes::Cache::PathError` instead.
- BREAKING CHANGE: Overhaul `ActiveSupport::Notifications`.
  - Only one notifation now: `cache.zk_recipes`. Removed
    `zk_recipes.cache.update` and `zk_recipes.cache.error`.
  - All notifications have a `path`.
  - Don't use notifications for unhandled exceptions.
- Development only: use `ZK_RECIPES_DEBUG=1 rspec` for debug logging.

## 0.1.0

- Initial release
