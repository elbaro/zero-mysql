# Changelog

## [v0.7.0](https://github.com/elbaro/zero-mysql/compare/v0.6.0...v0.7.0) - 2026-05-06

### <!-- 9 -->Other
- infra: update Rust crate rust_decimal to v1.42.0 ([#49](https://github.com/elbaro/zero-mysql/pull/49))
- infra: update Rust crate tokio to v1.52.2 ([#48](https://github.com/elbaro/zero-mysql/pull/48))
- infra: update Rust crate diesel to v2.3.9 ([#47](https://github.com/elbaro/zero-mysql/pull/47))
- infra: update Rust crate diesel to v2.3.8 ([#46](https://github.com/elbaro/zero-mysql/pull/46))
- infra: waive RUSTSEC-2024-0436 for transitive paste
- [**breaking**] infra!: update actions/upload-pages-artifact action to v5 ([#41](https://github.com/elbaro/zero-mysql/pull/41))
- infra: switch CI Rust toolchain from nightly to stable
- infra: use cargo-deny-action v2.0.17
- infra: replace rustsec/audit-check with cargo-deny-action
- infra: remove paths filter so audit required check reports on every PR
- infra: refresh Cargo.lock
- infra: add deny.toml
- infra: update Rust crate tokio to v1.52.1 ([#44](https://github.com/elbaro/zero-mysql/pull/44))
- infra: update non-breaking dependencies to v1.16.3 ([#43](https://github.com/elbaro/zero-mysql/pull/43))
- infra: update non-breaking dependencies ([#42](https://github.com/elbaro/zero-mysql/pull/42))
- infra: skip release commits in changelog parsers

## [v0.6.0](https://github.com/elbaro/zero-mysql/compare/v0.5.1...v0.6.0) - 2026-04-11

### <!-- 1 -->Bug fixes
- replace rsa crate with aws-lc-rs (RUSTSEC-2023-0071) ([#29](https://github.com/elbaro/zero-mysql/pull/29))

### <!-- 9 -->Other
- [**breaking**] infra!: update breaking dependencies ([#30](https://github.com/elbaro/zero-mysql/pull/30))
- tidy: unify renovate.json formatting across repos
- infra: disable Renovate dependency dashboard
- infra: run security audit on main push and PRs, not every branch push
- infra: update Rust crate tokio to v1.51.1 ([#38](https://github.com/elbaro/zero-mysql/pull/38))
- infra: update Rust crate tokio to v1.51.0 ([#37](https://github.com/elbaro/zero-mysql/pull/37))
- infra: update Rust crate mysql_async to v0.36.2 ([#36](https://github.com/elbaro/zero-mysql/pull/36))
- infra: update Rust crate zerocopy to v0.8.48 ([#35](https://github.com/elbaro/zero-mysql/pull/35))
- infra: update Rust crate rust_decimal to v1.41.0 ([#34](https://github.com/elbaro/zero-mysql/pull/34))
- infra: update Rust crate uuid to v1.23.0 ([#33](https://github.com/elbaro/zero-mysql/pull/33))
- infra: update non-breaking dependencies ([#32](https://github.com/elbaro/zero-mysql/pull/32))
- infra: update Rust crate zerocopy to v0.8.46 ([#31](https://github.com/elbaro/zero-mysql/pull/31))
- [**breaking**] infra!: update breaking dependencies ([#25](https://github.com/elbaro/zero-mysql/pull/25))
- infra: update non-breaking dependencies ([#28](https://github.com/elbaro/zero-mysql/pull/28))
- infra: enable platform automerge and remove schedule restriction
- infra: disable Renovate platformAutomerge
- tidy: move unwrap/expect clippy lints from Cargo.toml to lib.rs

## [v0.5.1](https://github.com/elbaro/zero-mysql/compare/v0.5.0...v0.5.1) - 2026-03-02

### <!-- 1 -->Bug fixes
- move separateMajorMinor to top-level config

### <!-- 9 -->Other
- infra: allow empty password in ci
- infra: change mariadb-12.3 to 12.3-rc
- tidy: replace panicking assertions with fallible check macros ([#26](https://github.com/elbaro/zero-mysql/pull/26))
- tidy: relax clippy lints, add clippy.toml, simplify const
- tidy: fix all clippy warnings with infallible conversions
- tidy: sync clippy lint set with zero-postgres
- infra: update Rust crate zerocopy to v0.8.40 ([#24](https://github.com/elbaro/zero-mysql/pull/24))
- infra: fix mariadb tests by adding MARIADB_ env vars
- infra: add multi-backend DB compatibility matrix to CI
- tidy: remove redundant test_ prefix from test functions
- tidy: use std::io::Error::other and fix redundant closures
- infra: grant checks:write permission to security audit
- infra: tune renovate config and normalize dep ranges
- infra: fix renovate hourly limit and branch splitting
- infra: group renovate PRs by breaking changes
- infra: configure renovate
- infra: add compile_fail doc tests for RefFromRow

## [v0.5.0](https://github.com/elbaro/zero-mysql/compare/v0.4.1...v0.5.0) - 2026-02-26

### <!-- 0 -->New features
- support caching_sha2_password authentication (MySQL 8.0+)
- compio, diesel
- zerocopy exec_foreach_ref

### <!-- 3 -->Documentation
- add feature flags

### <!-- 9 -->Other
- infra: fix typing in test
- infra: fix README.md title header
- release: zero-mysql-derive v0.2.0
- tidy: clippy
- infra: fix release PR body for multi-packages
- infra: fix release-plz for multi packages
- infra: fix release-plz config
- tidy: cleanup tracy_* examples
- [**breaking**] tidy!: rename feature flags
- [**breaking**] tidy!: rename FromRawRow to FromRow

## [v0.2.0](https://github.com/elbaro/zero-mysql/compare/derive-v0.1.0...derive-v0.2.0) - 2026-02-26

### <!-- 0 -->New features
- zerocopy exec_foreach_ref

### <!-- 9 -->Other
- Revert "release: zero-mysql-derive v0.2.0"
- release: zero-mysql-derive v0.2.0
- tidy: format
- [**breaking**] tidy!: rename FromRawRow to FromRow

## [v0.4.1](https://github.com/elbaro/zero-mysql/compare/v0.4.0...v0.4.1) - 2026-01-21

### <!-- 0 -->New features
- support [T], Vec<T> as params
- add support for common external crate types

### <!-- 3 -->Documentation
- revise datatype page
- add data type conversion

## [v0.4.0](https://github.com/elbaro/zero-mysql/compare/v0.3.1...v0.4.0) - 2026-01-11

### <!-- 1 -->Bug fixes
- [**breaking**] fix async closure API

### <!-- 3 -->Documentation
- simplification
- bulk command
- don't hard-code package version

## [v0.3.1](https://github.com/elbaro/zero-mysql/compare/v0.3.0...v0.3.1) - 2026-01-10

### <!-- 0 -->New features
- compiles on stable

## [v0.3.0](https://github.com/elbaro/zero-mysql/compare/v0.2.1...v0.3.0) - 2026-01-10

### <!-- 0 -->New features
- #[derive(FromRow)]
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
