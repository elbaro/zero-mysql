# Changelog

## [v0.3.1](https://github.com/elbaro/zero-mysql/compare/v0.3.0...v0.3.1) - 2026-01-10

### <!-- 0 -->New features
- compiles on stable

## [v0.3.0](https://github.com/elbaro/zero-mysql/compare/v0.2.1...v0.3.0) - 2026-01-10

### <!-- 0 -->New features
- #[derive(FromRawRow)]
- exec_foreach() which is useful for mapping tuples to structs

### <!-- 1 -->Bug fixes
- [**breaking**] exec_foreach closure can return Result
- point derive changelog to root CHANGELOG.md
- wrong release-plz config
- wrong release-plz config
- runs audit-check only on Cargo.* changes

### <!-- 3 -->Documentation
- remove api reference
- no API Reference
- explain mapping rows to structs
- clean up README
- link to docs.rs for connection options
- add mdbook and documentation links
- typos
- add tls feature flags

### <!-- 9 -->Other
- tidy: rename _typos.toml to typos.toml
- infra: add mdbook GitHub Pages workflow

## [v0.2.1](https://github.com/elbaro/zero-mysql/compare/v0.2.0...v0.2.1) - 2025-12-29

### <!-- 1 -->Bug fixes
- add rust-toolchain.toml to force nightly
- gate unix socket code behind cfg(unix)

## [v0.2.0](https://github.com/elbaro/zero-mysql/compare/v0.1.1...v0.2.0) - 2025-12-29

### <!-- 9 -->Other
- [**breaking**] tidy!: rename exec_bulk to exec_bulk_insert_or_update
- infra: remove reqray
- infra: release-plz config
- infra: fix release.yml
- infra: change release PR title
- infra: merge test/coverage steps
- infra: trusted publishing
- infra: setup release-plz
- ci
