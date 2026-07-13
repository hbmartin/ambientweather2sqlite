# Development

Pull requests and issue reports are welcome. For major changes, please open an
issue first to discuss what you would like to change.

## Setup

The project uses [uv](https://docs.astral.sh/uv/) for everything - use `uv run`
rather than calling `python` directly.

```bash
git clone https://github.com/hbmartin/ambientweather2sqlite.git
cd ambientweather2sqlite
uv sync --dev
```

## Checks

Run the formatter and linters after any change:

```bash
uv run ruff format ambientweather2sqlite tests
uv run ruff check ambientweather2sqlite tests --fix
uv run pyrefly check ambientweather2sqlite
uv run ty check ambientweather2sqlite
```

Run the full suite before opening a pull request:

```bash
uv run pyroma .                                    # package metadata
uv run deptry ambientweather2sqlite                # dependency hygiene
uv run lizard -Eduplicate ambientweather2sqlite    # complexity and duplication
uv run pytest tests/ --cov=ambientweather2sqlite   # tests; coverage must stay >= 92%
```

CI runs all of the above against Python 3.13 and 3.14 on every push and pull
request.

## Semgrep

Project-specific Semgrep rules protect the backup, Home Assistant discovery, and
health-metric invariants that generic Python linters cannot express. Run the
production scan and its rule fixtures with:

```bash
uvx --from semgrep==1.169.0 semgrep scan --metrics=off --error --config semgrep/ambientweather2sqlite.yml ambientweather2sqlite
uvx --from semgrep==1.169.0 semgrep --metrics=off --test semgrep/
```

## Conventions

- The source uses a flat `ambientweather2sqlite/` package layout, not a `src/` directory.
- Python 3.13+, and the package intentionally has **no runtime dependencies** - do not add any.
- `ambientweather2sqlite/mureq.py` is vendored. Preserve its public API and compare updates against upstream rather than replacing it wholesale.
- Tests are pytest-compatible `unittest.TestCase` classes. Use [Hypothesis](https://hypothesis.readthedocs.io/) for invariant and round-trip coverage where property-based tests fit.
- Type hints are first-class; prefer explicitness and small functions.
- Use modern Python features - assignment expressions, structural pattern matching - where they improve clarity.
- Use a parenthesized tuple of exception classes in every `except` clause, including for a single exception.
- Any user-facing change (e.g. a new CLI flag) must be documented in the `README.md`.

## Documentation

The documentation site is built with [Zensical](https://zensical.org/) from the
Markdown files in `docs/`, configured by `zensical.toml`.

```bash
uv run zensical serve   # live preview on http://localhost:8000
uv run zensical build   # render to site/
```

Pushes to `main` that touch the docs are deployed to GitHub Pages automatically
by `.github/workflows/docs.yml`.

## Core Architecture

<img src="assets/arch.svg" alt="Architecture diagram: CLI and configuration feed the daemon, which fetches from the station, parses observations, and writes to SQLite while optionally publishing to MQTT and serving the HTTP API." />

## Control Flow

<img src="assets/flow.svg" alt="Control flow diagram: the collection loop fetches live data, parses sensors, evolves the schema, inserts the observation, and sleeps until the next interval." />
