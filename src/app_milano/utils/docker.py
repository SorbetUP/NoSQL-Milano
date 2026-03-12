import os
import subprocess

from app_milano.config import ROOT


def run(cmd: list[str]) -> None:
    env = os.environ.copy()
    env["COMPOSE_IGNORE_ORPHANS"] = "true"
    compose_cmd = [
        "docker",
        "compose",
        "--env-file",
        ".env",
        "--project-directory",
        ".",
        *cmd[2:],
    ]
    subprocess.run(compose_cmd, cwd=ROOT, check=True, env=env)


def require_docker() -> None:
    try:
        subprocess.run(["docker", "info"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except FileNotFoundError as exc:
        raise SystemExit("Docker n'est pas installe.") from exc
    except subprocess.CalledProcessError as exc:
        raise SystemExit(
            "Le daemon Docker n'est pas demarre. Lance Docker Desktop puis relance src/app_milano/main.py."
        ) from exc


def start_mongo_service() -> None:
    run(["docker", "compose", "-f", "src/docker/mongo/docker-compose.yml", "up", "-d"])


def start_neo4j_service() -> None:
    run(["docker", "compose", "-f", "src/docker/neo4j/docker-compose.yml", "up", "-d"])


def start_services() -> None:
    start_mongo_service()
    start_neo4j_service()
