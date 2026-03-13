"""Generate and install macOS launchd service files."""

import shutil
import sys
from pathlib import Path

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
        <string>{executable}</string>
        <string>serve</string>
        <string>--config</string>
        <string>{config_path}</string>
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


def _find_executable() -> str:
    """Find the ambientweather2sqlite executable path."""
    if exe := shutil.which("ambientweather2sqlite"):
        return exe
    return sys.executable


def generate_plist(config_path: Path) -> str:
    """Generate a launchd plist XML string."""
    log_dir = Path.home() / "Library" / "Logs"
    return _PLIST_TEMPLATE.format(
        executable=_find_executable(),
        config_path=config_path.resolve(),
        log_dir=log_dir,
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
