# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Changed
- Split registry vs. connection modeling roles: `noteable.sql.connection.Connection` class vs `noteable.sql.connection.ConnectionRegistry` class.
- Fix bootstrap_datasource() passing along of create_engine_kwargs, otherwise misery.
- Defer data connection bootstrapping until first need, instead of at kernel launch time.
- Force all SQLAlchemy datasource subtypes to be their own individual concrete classes individually declaring their own needs_explicit_commit value for sanity, correctness, and clarity.
  - Stop expecting sqlmagic_autocommit being presented in gate-side metadata. Only consider the SQLAlchemy subtype's classlevel boolean. The field will be removed from Gate side in future work.
- Athena does _must not_  `quote_plus(connect_args['s3_staging_dir'])` -- they won't work encoded that way. It wants direct passing. Not sure where I got that idea.
- Reimplement schema introspection on top of neutral interface `InspectorProtocol`, based on SQLAlchemy's Inspector API, but not descending from it.

### Added
- `%ntbl change-log-level --rtu-level DEBUG` will update relevant Sending, PA, and Origami libraries to render useful debug logs related to RTU processing in PA

### Changed
- `%ntbl change-log-level` no longer requires `--app-level` arg. If it's not passed in, it won't change PA `planar_ally.*` log level from wherever it's currently set
- `@noteable` magic changed to use duckdb, away from sqlite.
- `%%sql @e456456 my_df << select a, b, c from foo` variable assignment syntax will now always return the resulting dataframe as well as silently assign to the interpreter variable (`my_df` in this case) as side-effect. Previously would only assign to the interpreter variable and announce the fact with a print(), while having no return result.
- Now use jinjasql for SQL cell template expansion, not simple string.Template.
- Simpler message printed as the cell's side-effect if an unknown datasource handle is attempted.
- Repackaged all code to be in 'noteable' toplevel package, not 'noteable_magics.'
- Better datasets download progress bars

### Fixed
- Don't mutate `metadata` passed to `bootstrap_datasource()`
  - Previously, a `KeyError` would be raised, hiding the underlying error when creating a connection

## [2.0.0] - 2022-03-15
### Changed
- Upgrade `ipython` to `^7.31.1` for security fix
- Upgrade `numpy` to `^1.22.2` for security fix

### Removed
- Remove `%ntbl` commands that interact with `git` directly. This will need to be added to the planar-ally api now. 
  - Remove `%ntbl diff project`
  - Remove `%ntbl status project`
- Remove no longer used `NTBLMagic` config values: `git_user_name` and `git_user_email`

## [1.2.4] - 2021-09-01
### Added
- Added `change-log-level` command to change planar-ally's log level via API call

### Fixed
- Initialize `@noteable` sql connection when registering the magics

## [1.2.3] - 2021-08-10
### Added
- Log unexpected errors in the catch_em_all decorator

### Fixed
- %ntbl magic now ensures that the project directory exists instead of crashing when it doesn't

## [1.2.2] - 2021-07-27
### Fixed
- Explicitly use `sep=` kwarg for `pd.read_csv` to remove `FutureWarning`
- Don't include dataframe index in SQL from `%create_or_replace_data_view` unless explicitly set with `--include-index`
- Throw an error to stop cell execution when `NTBLMagic` fails

## [1.2.1] - 2021-07-19
### Fixed
- Use `httpx` streams properly

## [1.2.0] - 2021-07-19
### Changed
- `create_or_replace_data_view` handles filenames with spaces (either escaped or
  quoted).

### Added
- `%ntbl push datasets [PATH]` command
- `%ntbl pull datasets [PATH]` command
- Logging to `/var/log/noteable_magics.log` or locally to `/tmp/noteable_magics.log`

### Fixed
- Use `httpx` instead of `requests` for better http streaming support

## [1.1.0] - 2021-06-14
### Changed
- Updated magics to use `planar-ally` as the sidecar with HTTP+REST

## [1.0.0] - 2021-05-04
### Added
- This changelog

### Changed
- Send a `FileType` with all protobuf messages
    - This allows us to filter operations by this field rather than the prefix
