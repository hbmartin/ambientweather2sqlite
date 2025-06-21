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


def create_config_file(config_path: str | Path | None) -> Path:
    if (
        config_path is not None
        and (output_path := Path(config_path))
        and output_path.exists()
    ):
        return output_path

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
    port = input(
        "Enter port number to server JSON data (leave blank to disable):\n",
    ).strip()

    output_file = (
        f"{current_path}/aw2sqlite.toml" if config_path is None else config_path
    )
    if config_path is None:
        output_file = input(
            f"Enter output TOML filename (leave blank for default: {output_file}):\n",
        ).strip()

    config = f'live_data_url = "{ambient_url}"\ndatabase_path = "{database_path}"\n'
    if port:
        config += f"port = {port}\n"
    output_path = Path(output_file)
    output_path.write_text(config)

    return output_path
