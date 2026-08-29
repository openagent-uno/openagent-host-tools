"""Read release metadata from the package's single authoritative source."""

from __future__ import annotations

import runpy
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROJECT_VERSION = str(
    runpy.run_path(str(ROOT / "src/openagent_host_tools/_version.py"))["__version__"]
)
