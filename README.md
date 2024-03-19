# lmdbnav

A terminal UI to inspect LMDB database files.

This is a tool that we use internally to debug LMDB databases. We decided to Open Source it, since it can be useful to others.

Contributions are welcome.

## Installation

```
$ GOBIN=/path/to/target/bin go install ./cmd/lmdbnav
```

## Usage

```
$ lmdbnav /path/to/lmdb
```

To enables support for [Lightning Stream](https://github.com/PowerDNS/lmdbnav) headers:

```
$ lmdbnav --ls /path/to/lmdb
```

Key bindings:

- Arrow keys are used for navigation
- `Enter` dives into a subview
- `Esc` returns to the previous screen
- `R` or `r` refreshes the view from the LMDB
- `[` and `]` jump 1000 rows
- `{` and `}` jump 10000 rows
- `Home` and `End` navigate to the beginning and end

## Example screenshots

![Databases](screenshots/databases.png)

![Data](screenshots/data.png)

![Inspect](screenshots/inspect.png)
