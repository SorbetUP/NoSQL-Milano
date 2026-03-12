from __future__ import annotations

import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SRC_DIR = ROOT / "src"
ENV_PATH = ROOT / ".env"

def load_env_file() -> None:
    if not ENV_PATH.exists():
        raise SystemExit("Le fichier .env est manquant.")

    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ[key.strip()] = value.strip()


load_env_file()

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nosql_milano.app import run_cli


if __name__ == "__main__":
    run_cli()
