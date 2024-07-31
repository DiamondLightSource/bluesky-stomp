import subprocess
import sys

from bluesky_stomp import __version__


def test_cli_version():
    cmd = [sys.executable, "-m", "bluesky_stomp", "--version"]
    assert subprocess.check_output(cmd).decode().strip() == __version__
