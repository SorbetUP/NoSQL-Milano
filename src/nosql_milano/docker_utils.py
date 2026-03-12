from __future__ import annotations

import subprocess

from .config import ROOT


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True)


def require_docker() -> None:
    try:
        subprocess.run(["docker", "info"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except FileNotFoundError as exc:
        raise SystemExit("Docker n'est pas installe.") from exc
    except subprocess.CalledProcessError as exc:
        raise SystemExit(
            "Le daemon Docker n'est pas demarre. Lance Docker Desktop ou Colima puis relance bootstrap.py."
        ) from exc


def start_services() -> None:
    run(["docker", "compose", "up", "-d"])
