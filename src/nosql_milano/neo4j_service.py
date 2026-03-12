from __future__ import annotations

import time
from typing import Any, Iterable

from neo4j import Driver, GraphDatabase


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


def batch_rows(rows: list[dict[str, Any]], size: int = 200) -> Iterable[list[dict[str, Any]]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def import_graph(driver: Driver, users: list[dict[str, Any]], tweets: list[dict[str, Any]]) -> None:
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
def get_milanoops_followers(driver: Driver) -> list[dict[str, Any]]:
    pass


# Question 8: utilisateurs suivis par MilanoOps
def get_milanoops_following(driver: Driver) -> list[dict[str, Any]]:
    pass


# Question 9: relations reciproques avec MilanoOps
def get_mutual_connections_with_milanoops(driver: Driver) -> list[dict[str, Any]]:
    pass


# Question 10: utilisateurs avec plus de 10 followers
def get_users_with_more_than_ten_followers(driver: Driver) -> list[dict[str, Any]]:
    pass


# Question 11: utilisateurs qui suivent plus de 5 utilisateurs
def get_users_following_more_than_five_users(driver: Driver) -> list[dict[str, Any]]:
    pass


# Question 14: tweets qui initient une discussion
def get_thread_starters(driver: Driver) -> list[dict[str, Any]]:
    pass


# Question 15: discussion la plus longue
def get_longest_discussion(driver: Driver) -> dict[str, Any]:
    pass


# Question 16: debut et fin de chaque conversation
def get_conversation_start_and_end(driver: Driver) -> list[dict[str, Any]]:
    pass
