from pathlib import Path

current_path = Path.cwd()


def get_config_path() -> Path | None:
    cwd_config = current_path / "aw2sqlite.toml"
    if cwd_config.exists():
        return cwd_config
    home_config = Path.home() / ".aw2sqlite.toml"
    if home_config.exists():
        return home_config
    return None


def create_config_file() -> Path:
    print("Configuration Setup")
    print("-" * 20)

    ambient_url = ""
    while not ambient_url.startswith("http"):
        ambient_url = input(
            "Enter AmbientWeather Live Data URL: (e.g. http://192.168.0.226/livedata.htm)\n",
        ).strip()

    database_path = input(
        f"Enter Database Path (leave blank for default: {current_path}/aw2sqlite.db):\n",
    ).strip()
    if not database_path:
        database_path = f"{current_path}/aw2sqlite.db"

    output_file = input(
        f"Enter output TOML filename (leave blank for default: {current_path}/aw2sqlite.toml):\n",
    ).strip()
    if not output_file:
        output_file = f"{current_path}/aw2sqlite.toml"

    # Ensure .toml extension
    if not output_file.endswith(".toml"):
        output_file += ".toml"

    output_path = Path(output_file)
    output_path.write_text(
        f'live_data_url = "{ambient_url}"\ndatabase_path = "{database_path}"\n',
    )

    return output_path
