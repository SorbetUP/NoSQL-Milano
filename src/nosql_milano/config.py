from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"


@dataclass(frozen=True)
class Settings:
    compose_project_name: str
    mongo_port: str
    mongo_root_username: str
    mongo_root_password: str
    mongo_app_db: str
    mongo_app_username: str
    mongo_app_password: str
    neo4j_http_port: str
    neo4j_bolt_port: str
    neo4j_user: str
    neo4j_password: str

    @property
    def mongo_root_uri(self) -> str:
        return (
            f"mongodb://{self.mongo_root_username}:{self.mongo_root_password}"
            f"@localhost:{self.mongo_port}/admin"
        )

    @property
    def mongo_app_uri(self) -> str:
        return (
            f"mongodb://{self.mongo_app_username}:{self.mongo_app_password}"
            f"@localhost:{self.mongo_port}/{self.mongo_app_db}?authSource={self.mongo_app_db}"
        )

    @property
    def neo4j_bolt_uri(self) -> str:
        return f"bolt://localhost:{self.neo4j_bolt_port}"

    @property
    def neo4j_browser_url(self) -> str:
        return f"http://localhost:{self.neo4j_http_port}"


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"La variable d'environnement {name} est manquante.")
    return value


def load_settings() -> Settings:
    return Settings(
        compose_project_name=require_env("COMPOSE_PROJECT_NAME"),
        mongo_port=require_env("MONGO_PORT"),
        mongo_root_username=require_env("MONGO_ROOT_USERNAME"),
        mongo_root_password=require_env("MONGO_ROOT_PASSWORD"),
        mongo_app_db=require_env("MONGO_APP_DB"),
        mongo_app_username=require_env("MONGO_APP_USERNAME"),
        mongo_app_password=require_env("MONGO_APP_PASSWORD"),
        neo4j_http_port=require_env("NEO4J_HTTP_PORT"),
        neo4j_bolt_port=require_env("NEO4J_BOLT_PORT"),
        neo4j_user=require_env("NEO4J_USER"),
        neo4j_password=require_env("NEO4J_PASSWORD"),
    )
