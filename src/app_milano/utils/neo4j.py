import time

from neo4j import Driver, GraphDatabase

from app_milano.config import load_env_file, load_settings


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


def import_graph(driver: Driver, users: list[dict], tweets: list[dict]) -> None:
    constraints = [
        "CREATE CONSTRAINT user_user_id IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE",
        "CREATE CONSTRAINT tweet_tweet_id IF NOT EXISTS FOR (t:Tweet) REQUIRE t.tweet_id IS UNIQUE",
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

        for batch in batch_rows(tweets):
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


# Question 7: followers de MilanoOps
def get_milanoops_followers(driver: Driver) -> list[dict]:
    pass


# Question 8: utilisateurs suivis par MilanoOps
def get_milanoops_following(driver: Driver) -> list[dict]:
    pass


# Question 9: relations reciproques avec MilanoOps
def get_mutual_connections_with_milanoops(driver: Driver) -> list[dict]:
    pass


# Question 10: utilisateurs avec plus de 10 followers
def get_users_with_more_than_ten_followers(driver: Driver) -> list[dict]:
    pass


# Question 11: utilisateurs qui suivent plus de 5 utilisateurs
def get_users_following_more_than_five_users(driver: Driver) -> list[dict]:
    pass


# Question 14: tweets qui initient une discussion
def get_thread_starters(driver: Driver) -> list[dict]:
    pass


# Question 15: discussion la plus longue
def get_longest_discussion(driver: Driver) -> dict:
    pass


# Question 16: debut et fin de chaque conversation
def get_conversation_start_and_end(driver: Driver) -> list[dict]:
    pass


def create_neo4j_context(placeholder="in progress"):
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
