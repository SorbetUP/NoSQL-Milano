from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Iterable

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import PyMongoError


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
ENV_PATH = ROOT / ".env"

DEFAULT_ENV = {
    "COMPOSE_PROJECT_NAME": "nosql_milano",
    "MONGO_PORT": "27019",
    "MONGO_ROOT_USERNAME": "root",
    "MONGO_ROOT_PASSWORD": "root_password",
    "MONGO_APP_DB": "milano2026",
    "MONGO_APP_USERNAME": "app_user",
    "MONGO_APP_PASSWORD": "app_password",
    "NEO4J_HTTP_PORT": "7474",
    "NEO4J_BOLT_PORT": "7687",
    "NEO4J_USER": "neo4j",
    "NEO4J_PASSWORD": "neo4j_password",
}


def ensure_env_file() -> dict[str, str]:
    values = DEFAULT_ENV.copy()
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            values[key.strip()] = value.strip()
    else:
        content = "\n".join(f"{key}={value}" for key, value in values.items()) + "\n"
        ENV_PATH.write_text(content, encoding="utf-8")
    return values


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True)


def require_docker() -> None:
    try:
        subprocess.run(["docker", "info"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except FileNotFoundError:
        raise SystemExit("Docker n'est pas installe.")
    except subprocess.CalledProcessError:
        raise SystemExit("Le daemon Docker n'est pas demarre. Lance Docker Desktop ou Colima puis relance bootstrap.py.")


def wait_for_mongo(uri: str, timeout: int = 90) -> MongoClient:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=3000)
            client.admin.command("ping")
            return client
        except PyMongoError:
            time.sleep(2)
    raise SystemExit("MongoDB n'est pas pret apres 90 secondes.")


def wait_for_neo4j(uri: str, user: str, password: str, timeout: int = 120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        try:
            with driver.session() as session:
                session.run("RETURN 1").consume()
            return driver
        except Exception:
            driver.close()
            time.sleep(3)
    raise SystemExit("Neo4j n'est pas pret apres 120 secondes.")


def ensure_mongo_user(client: MongoClient, env: dict[str, str]) -> None:
    app_db = client[env["MONGO_APP_DB"]]
    username = env["MONGO_APP_USERNAME"]
    password = env["MONGO_APP_PASSWORD"]
    user_info = app_db.command("usersInfo", username)
    if user_info.get("users"):
        app_db.command(
            "updateUser",
            username,
            pwd=password,
            roles=[{"role": "readWrite", "db": env["MONGO_APP_DB"]}],
        )
    else:
        app_db.command(
            "createUser",
            username,
            pwd=password,
            roles=[{"role": "readWrite", "db": env["MONGO_APP_DB"]}],
        )


def setup_mongo(client: MongoClient, env: dict[str, str]) -> None:
    ensure_mongo_user(client, env)
    db = client[env["MONGO_APP_DB"]]
    users = json.loads((DATA_DIR / "users.json").read_text(encoding="utf-8"))
    tweets = json.loads((DATA_DIR / "tweets.json").read_text(encoding="utf-8"))

    db.users.delete_many({})
    db.tweets.delete_many({})

    if users:
        db.users.insert_many(users, ordered=True)
    if tweets:
        db.tweets.insert_many(tweets, ordered=True)

    db.users.create_index([("user_id", ASCENDING)], unique=True)
    db.users.create_index([("username", ASCENDING)], unique=True)
    db.users.create_index([("role", ASCENDING)])
    db.users.create_index([("country", ASCENDING)])

    db.tweets.create_index([("tweet_id", ASCENDING)], unique=True)
    db.tweets.create_index([("user_id", ASCENDING)])
    db.tweets.create_index([("hashtags", ASCENDING)])
    db.tweets.create_index([("in_reply_to_tweet_id", ASCENDING)])
    db.tweets.create_index([("created_at", ASCENDING)])
    db.tweets.create_index([("favorite_count", DESCENDING)])


def batches(rows: list[dict], size: int = 200) -> Iterable[list[dict]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def setup_neo4j(driver, users: list[dict], tweets: list[dict]) -> None:
    constraints = [
        "CREATE CONSTRAINT user_user_id IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE",
        "CREATE CONSTRAINT tweet_tweet_id IF NOT EXISTS FOR (t:Tweet) REQUIRE t.tweet_id IS UNIQUE",
        "CREATE INDEX user_username IF NOT EXISTS FOR (u:User) ON (u.username)",
    ]

    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n").consume()
        for statement in constraints:
            session.run(statement).consume()

        for batch in batches(users):
            session.run(
                """
                UNWIND $rows AS row
                MERGE (u:User {user_id: row.user_id})
                SET u.username = row.username,
                    u.role = row.role,
                    u.country = row.country,
                    u.created_at = row.created_at
                """,
                rows=batch,
            ).consume()

        for batch in batches(tweets):
            session.run(
                """
                UNWIND $rows AS row
                MERGE (t:Tweet {tweet_id: row.tweet_id})
                SET t.user_id = row.user_id,
                    t.text = row.text,
                    t.hashtags = row.hashtags,
                    t.created_at = row.created_at,
                    t.favorite_count = row.favorite_count,
                    t.in_reply_to_tweet_id = row.in_reply_to_tweet_id
                """,
                rows=batch,
            ).consume()

        session.run(
            """
            MATCH (u:User)
            MATCH (t:Tweet {user_id: u.user_id})
            MERGE (u)-[:AUTHORED]->(t)
            """
        ).consume()

        session.run(
            """
            MATCH (t:Tweet)
            WHERE t.in_reply_to_tweet_id IS NOT NULL
            MATCH (parent:Tweet {tweet_id: t.in_reply_to_tweet_id})
            MERGE (t)-[:REPLY_TO]->(parent)
            """
        ).consume()


def main() -> None:
    env = ensure_env_file()
    require_docker()

    run(["docker", "compose", "up", "-d"])

    mongo_root_uri = (
        f"mongodb://{env['MONGO_ROOT_USERNAME']}:{env['MONGO_ROOT_PASSWORD']}"
        f"@localhost:{env['MONGO_PORT']}/admin"
    )
    mongo_app_uri = (
        f"mongodb://{env['MONGO_APP_USERNAME']}:{env['MONGO_APP_PASSWORD']}"
        f"@localhost:{env['MONGO_PORT']}/{env['MONGO_APP_DB']}?authSource={env['MONGO_APP_DB']}"
    )
    neo4j_bolt = f"bolt://localhost:{env['NEO4J_BOLT_PORT']}"
    neo4j_browser = f"http://localhost:{env['NEO4J_HTTP_PORT']}"

    mongo_client = wait_for_mongo(mongo_root_uri)
    setup_mongo(mongo_client, env)

    users = json.loads((DATA_DIR / "users.json").read_text(encoding="utf-8"))
    tweets = json.loads((DATA_DIR / "tweets.json").read_text(encoding="utf-8"))

    neo4j_driver = wait_for_neo4j(neo4j_bolt, env["NEO4J_USER"], env["NEO4J_PASSWORD"])
    setup_neo4j(neo4j_driver, users, tweets)
    neo4j_driver.close()
    mongo_client.close()

    print("Bootstrap termine.")
    print()
    print("MongoDB")
    print(mongo_app_uri)
    print()
    print("Neo4j")
    print(f"Browser: {neo4j_browser}")
    print(f"Bolt: {neo4j_bolt}")
    print(f"User: {env['NEO4J_USER']}")
    print(f"Password: {env['NEO4J_PASSWORD']}")


if __name__ == "__main__":
    try:
        main()
    except (PyMongoError, Neo4jError, subprocess.CalledProcessError) as exc:
        raise SystemExit(str(exc)) from exc
