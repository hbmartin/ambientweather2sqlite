"""Generate and install systemd user service files."""

import shlex
import shutil
import sys
from pathlib import Path

_UNIT_TEMPLATE = """\
[Unit]
Description=AmbientWeather to SQLite collector
Documentation=https://github.com/hbmartin/ambientweather2sqlite
After=network-online.target
Wants=network-online.target

[Service]
ExecStart={exec_start}
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
"""

_UNIT_NAME = "ambientweather2sqlite.service"


def _command_arguments(config_path: Path) -> list[str]:
    """Build a service command that works with both scripts and Python runtimes."""
    for executable_name in ("aw2sqlite", "ambientweather2sqlite"):
        if executable_path := shutil.which(executable_name):
            return [executable_path, "serve", "--config", str(config_path.resolve())]

    return [
        sys.executable,
        "-m",
        "ambientweather2sqlite",
        "serve",
        "--config",
        str(config_path.resolve()),
    ]


def generate_unit(config_path: Path) -> str:
    """Generate a systemd user service unit string."""
    exec_start = shlex.join(_command_arguments(config_path)).replace("%", "%%")
    return _UNIT_TEMPLATE.format(exec_start=exec_start)


def install_systemd(config_path: Path) -> Path:
    """Generate and write a systemd user service unit file.

    Returns:
        Path to the written unit file.

    """
    unit_content = generate_unit(config_path)
    unit_dir = Path.home() / ".config" / "systemd" / "user"
    unit_path = unit_dir / _UNIT_NAME

    unit_dir.mkdir(parents=True, exist_ok=True)
    unit_path.write_text(unit_content, encoding="utf-8")
    print(f"Unit file written to {unit_path}")
    print("\nTo start the service now and on login:")
    print("  systemctl --user daemon-reload")
    print(f"  systemctl --user enable --now {_UNIT_NAME}")
    print("\nTo stop the service:")
    print(f"  systemctl --user disable --now {_UNIT_NAME}")
    print("\nTo keep it running after logout:")
    print("  loginctl enable-linger $USER")

    return unit_path
