import csv
import json
import time

from neo4j import Driver, GraphDatabase

from app_milano.config import DATA_DIR, load_env_file, load_settings


def wait_for_neo4j(uri: str, user: str, password: str, timeout: int = 120) -> Driver:
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


def batch_rows(rows: list[dict], size: int = 200):
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def load_follows() -> list[dict]:
    follows_path = DATA_DIR / "follows.csv"
    if not follows_path.exists():
        return []
    with follows_path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def load_users() -> list[dict]:
    users_path = DATA_DIR / "users.json"
    if not users_path.exists():
        return []
    return json.loads(users_path.read_text(encoding="utf-8"))


def import_graph(driver: Driver, users: list[dict], follows: list[dict]) -> None:
    constraints = [
        "CREATE CONSTRAINT user_user_id IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE",
        "CREATE INDEX user_username IF NOT EXISTS FOR (u:User) ON (u.username)",
    ]

    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n").consume()
        for statement in constraints:
            session.run(statement).consume()

        for batch in batch_rows(users):
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

        for batch in batch_rows(follows):
            session.run(
                """
                UNWIND $rows AS row
                MATCH (source:User {user_id: row.source_user_id})
                MATCH (target:User {user_id: row.target_user_id})
                WHERE source.user_id <> target.user_id
                MERGE (source)-[:FOLLOWS]->(target)
                """,
                rows=batch,
            ).consume()


# Question 7: followers de MilanoOps
def get_milanoops_followers(driver: Driver) -> list[dict]:
    with driver.session() as session:
        result = session.run(
            """
            MATCH (user:User)-[:FOLLOWS]->(:User {username: 'MilanoOps'})
            RETURN user.user_id AS user_id,
                   user.username AS username,
                   user.role AS role,
                   user.country AS country
            ORDER BY user.username
            """
        )
        return [record.data() for record in result]


# Question 8: utilisateurs suivis par MilanoOps
def get_milanoops_following(driver: Driver) -> list[dict]:
    with driver.session() as session:
        result = session.run(
            """
            MATCH (:User {username: 'MilanoOps'})-[:FOLLOWS]->(user:User)
            RETURN user.user_id AS user_id,
                   user.username AS username,
                   user.role AS role,
                   user.country AS country
            ORDER BY user.username
            """
        )
        return [record.data() for record in result]


# Question 9: relations reciproques avec MilanoOps
def get_mutual_connections_with_milanoops(driver: Driver) -> list[dict]:
    with driver.session() as session:
        result = session.run(
            """
            MATCH (:User {username: 'MilanoOps'})-[:FOLLOWS]->(user:User)-[:FOLLOWS]->(:User {username: 'MilanoOps'})
            RETURN user.user_id AS user_id,
                   user.username AS username,
                   user.role AS role,
                   user.country AS country
            ORDER BY user.username
            """
        )
        return [record.data() for record in result]


# Question 10: utilisateurs avec plus de 10 followers
def get_users_with_more_than_ten_followers(driver: Driver) -> list[dict]:
    with driver.session() as session:
        result = session.run(
            """
            MATCH (user:User)<-[:FOLLOWS]-(follower:User)
            WITH user, count(follower) AS follower_count
            WHERE follower_count > 10
            RETURN user.user_id AS user_id,
                   user.username AS username,
                   user.role AS role,
                   user.country AS country,
                   follower_count
            ORDER BY follower_count DESC, user.username
            """
        )
        return [record.data() for record in result]


# Question 11: utilisateurs qui suivent plus de 5 utilisateurs
def get_users_following_more_than_five_users(driver: Driver) -> list[dict]:
    with driver.session() as session:
        result = session.run(
            """
            MATCH (user:User)-[:FOLLOWS]->(followed:User)
            WITH user, count(followed) AS following_count
            WHERE following_count > 5
            RETURN user.user_id AS user_id,
                   user.username AS username,
                   user.role AS role,
                   user.country AS country,
                   following_count
            ORDER BY following_count DESC, user.username
            """
        )
        return [record.data() for record in result]


def create_neo4j_context(placeholder="Indisponible"):
    context = {
        "placeholder": placeholder,
        "settings": None,
        "driver": None,
    }
    if not load_env_file(required=False):
        return context

    try:
        context["settings"] = load_settings()
        context["driver"] = GraphDatabase.driver(
            context["settings"].neo4j_bolt_uri,
            auth=(context["settings"].neo4j_user, context["settings"].neo4j_password),
        )
        with context["driver"].session() as session:
            session.run("RETURN 1").consume()
            user_count = session.run("MATCH (u:User) RETURN count(u) AS value").single()["value"]
            follows_count = session.run("MATCH ()-[r:FOLLOWS]->() RETURN count(r) AS value").single()["value"]
        if user_count == 0 or follows_count == 0:
            import_graph(context["driver"], load_users(), load_follows())
    except Exception:
        if context["driver"]:
            context["driver"].close()
        context["driver"] = None
    return context


def close_neo4j_context(context):
    if context and context["driver"]:
        context["driver"].close()


def _normalize_result(context, value):
    if value is None:
        return context["placeholder"]
    if isinstance(value, dict) and not value:
        return context["placeholder"]
    if isinstance(value, list) and not value:
        return context["placeholder"]
    return value


def get_ui_q7_followers(context):
    if not context["driver"]:
        return context["placeholder"]
    return _normalize_result(context, get_milanoops_followers(context["driver"]))


def get_ui_q8_following(context):
    if not context["driver"]:
        return context["placeholder"]
    return _normalize_result(context, get_milanoops_following(context["driver"]))


def get_ui_q9_mutual_connections(context):
    if not context["driver"]:
        return context["placeholder"]
    return _normalize_result(context, get_mutual_connections_with_milanoops(context["driver"]))


def get_ui_q10_users_with_more_than_ten_followers(context):
    if not context["driver"]:
        return context["placeholder"]
    return _normalize_result(context, get_users_with_more_than_ten_followers(context["driver"]))


def get_ui_q11_users_following_more_than_five_users(context):
    if not context["driver"]:
        return context["placeholder"]
    return _normalize_result(context, get_users_following_more_than_five_users(context["driver"]))
