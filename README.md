# AmbientWeather to SQLite

[![PyPI](https://img.shields.io/pypi/v/ambientweather2sqlite.svg)](https://pypi.org/project/ambientweather2sqlite/)
[![Lint](https://github.com/hbmartin/ambientweather2sqlite/actions/workflows/lint.yml/badge.svg)](https://github.com/hbmartin/ambientweather2sqlite/actions/workflows/lint.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Code style: black](https://img.shields.io/badge/🐧️-black-000000.svg)](https://github.com/psf/black)
[![Checked with pyrefly](https://img.shields.io/badge/🪲-pyrefly-fe8801.svg)](https://pyrefly.org/)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/hbmartin/graphviz2drawio)

A project to record minute-by-minute weather observations from an AmbientWeather station over the local network - no API needed!

## Key Features

* Local Network Operation: Direct connection to weather stations without external API dependencies
* Continuous Data Collection: Automated daemon process collecting data at 60-second intervals
* Dynamic Schema Management: Automatic database schema evolution as new sensors are detected
* HTTP JSON API: Optional web server providing live data access
* Interactive Configuration: Command-line setup wizard for initial configuration
* Cross-Platform Distribution: Available via PyPI with pipx installation

## Installation

* macOS: `brew install pipx && pipx install ambientweather2sqlite`
* Ubuntu / Debian: `sudo apt update && sudo apt install pipx && pipx install ambientweather2sqlite`
* Fedora: `sudo dnf install pipx && pipx install ambientweather2sqlite`

## Setup

On the first run of `ambientweather2sqlite` you will be asked to provide the station's LiveData URL and the database path.

This config file is saved to your current directory by default but may be stored anywhere.

On subsequent runs, you can pass the file name as a command line argument or it will be automatically detected in your current directory or at `~/.aw2sqlite.toml`

## Legal

© [Harold Martin](https://www.linkedin.com/in/harold-martin-98526971/) - released under [GPLv3](LICENSE.md)

AmbientWeather is a trademark of Ambient, LLC.