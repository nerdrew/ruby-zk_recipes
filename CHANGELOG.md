# Changelog

## 0.2.0

BREAKING CHANGES

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
