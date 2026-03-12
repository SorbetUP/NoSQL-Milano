from __future__ import annotations

from .config import Settings
from .mongo_service import MongoQuestionResults


def print_connection_info(settings: Settings) -> None:
    print("Bootstrap termine.")
    print()
    print("MongoDB")
    print(settings.mongo_app_uri)
    print()
    print("Neo4j")
    print(f"Browser: {settings.neo4j_browser_url}")
    print(f"Bolt: {settings.neo4j_bolt_uri}")
    print(f"User: {settings.neo4j_user}")
    print(f"Password: {settings.neo4j_password}")


def print_question_results(results: MongoQuestionResults) -> None:
    print()
    print("Questions MongoDB")
    print(f"Q1 - Nombre d'utilisateurs : {results.user_count}")
    print(f"Q2 - Nombre de tweets : {results.tweet_count}")
    print(f"Q3 - Nombre de hashtags distincts : {results.distinct_hashtag_count}")

    print()
    print("Q12 - Top 10 tweets les plus populaires")
    for index, tweet in enumerate(results.top_tweets, start=1):
        text = " ".join(tweet["text"].split())
        preview = text[:90] + ("..." if len(text) > 90 else "")
        print(
            f"{index}. {tweet['tweet_id']} | @{tweet.get('username', 'unknown')} | "
            f"{tweet['favorite_count']} likes | {preview}"
        )

    print()
    print("Q13 - Top 10 hashtags les plus populaires")
    for index, hashtag in enumerate(results.top_hashtags, start=1):
        print(f"{index}. #{hashtag['hashtag']} | {hashtag['tweet_count']} tweets")
