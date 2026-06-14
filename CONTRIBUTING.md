# Contributing

Thanks for your interest in Outboxer. Contributions are welcome.

This is a small, best-effort project. Issues and pull requests are read and
reviewed when time allows, and may be declined if they don't fit the project's
scope. Opening an issue to discuss a non-trivial change before writing code is
appreciated.

## Development

You need [Go](https://go.dev) (see the version in [`go.mod`](go.mod)) and,
optionally, [`just`](https://github.com/casey/just) and Docker for the
integration tests.

Run the unit tests:

```sh
just test          # or: go test ./...
```

Run every check the CI runs (formatting, vet, lint, vulnerability scan, tests,
build):

```sh
just check
```

Run the integration tests against a throwaway PostgreSQL in Docker:

```sh
just integration
```

`just ci` runs the full suite, including the integration tests.

## Pull requests

- Keep changes focused; one logical change per pull request.
- Make sure `just check` passes before opening the pull request.
- Add or update tests for behavior changes.
- Update the `README.md` when you change configuration or behavior.

## License

By contributing, you agree that your contributions are licensed under the
[MIT License](LICENSE), the same license as the project.
