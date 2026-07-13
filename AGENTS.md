# Codex

- The source uses a flat `ambientweather2sqlite/` package layout, not a `src/` directory.
- The project supports Python 3.13+ and intentionally has no runtime dependencies.
- `ambientweather2sqlite/mureq.py` is vendored; preserve its public API and compare updates with upstream instead of replacing it wholesale.
- Tests use pytest-compatible `unittest.TestCase` classes. Use Hypothesis for invariant and round-trip coverage where property-based tests fit.
- Always run `uv run ruff format ambientweather2sqlite tests` and `uv run ruff check ambientweather2sqlite tests --fix` after making changes.
- Run `uv run pyrefly check ambientweather2sqlite` and `uv run ty check ambientweather2sqlite` after making changes.
- Run `uv run pyroma .`, `uv run deptry ambientweather2sqlite`, and `uv run lizard -Eduplicate ambientweather2sqlite` after finishing implementation.
- Run `uv run pytest tests/ --cov=ambientweather2sqlite` after finishing implementation; coverage must remain at or above the configured threshold.
- Any user-facing changes (e.g. new CLI flags) should be documented in the `README.md`.
- Use `uv` not `python` for running scripts.
- Treat type hints as first-class.
- Prefer explicitness and small functions.
- Use modern Python features, such as assignment expressions and structural pattern matching, where they improve clarity.
- Use a parenthesized tuple of exception classes in every `except` clause, including for a single exception.
