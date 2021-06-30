# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- `%ntbl push datasets [PATH]` command
- `%ntbl pull datasets [PATH]` command
- Logging to `/var/log/noteable_magics.log` or locally to `/tmp/noteable_magics.log`

## [1.1.0] - 2021-06-14
### Changed
- Updated magics to use `planar-ally` as the sidecar with HTTP+REST

## [1.0.0] - 2021-05-04
### Added
- This changelog

### Changed
- Send a `FileType` with all protobuf messages
    - This allows us to filter operations by this field rather than the prefix
