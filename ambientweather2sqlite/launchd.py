"""Generate and install macOS launchd service files."""

import shutil
import sys
from pathlib import Path
from xml.sax.saxutils import escape

_PLIST_TEMPLATE = """\
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" \
"http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.ambientweather2sqlite</string>
    <key>ProgramArguments</key>
    <array>
{program_arguments}
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{log_dir}/ambientweather2sqlite.stdout.log</string>
    <key>StandardErrorPath</key>
    <string>{log_dir}/ambientweather2sqlite.stderr.log</string>
</dict>
</plist>
"""

_LABEL = "com.ambientweather2sqlite"


def _program_arguments(config_path: Path) -> list[str]:
    """Build a launchd command that works with both scripts and Python runtimes."""
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


def _render_program_arguments(program_arguments: list[str]) -> str:
    return "\n".join(
        f"        <string>{escape(argument)}</string>" for argument in program_arguments
    )


def generate_plist(config_path: Path) -> str:
    """Generate a launchd plist XML string."""
    log_dir = Path.home() / "Library" / "Logs"
    return _PLIST_TEMPLATE.format(
        program_arguments=_render_program_arguments(_program_arguments(config_path)),
        log_dir=escape(str(log_dir)),
    )


def install_launchd(config_path: Path) -> Path:
    """Generate and write a macOS launchd service file.

    Returns:
        Path to the written plist file.

    """
    plist_content = generate_plist(config_path)
    plist_dir = Path.home() / "Library" / "LaunchAgents"
    plist_path = plist_dir / f"{_LABEL}.plist"

    plist_dir.mkdir(parents=True, exist_ok=True)
    plist_path.write_text(plist_content, encoding="utf-8")
    print(f"Plist written to {plist_path}")
    print("\nTo load the service:")
    print(f"  launchctl load {plist_path}")
    print("\nTo unload the service:")
    print(f"  launchctl unload {plist_path}")

    return plist_path
