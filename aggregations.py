from __future__ import annotations

from pathlib import Path
from pprint import pprint

from pymongo import MongoClient


ROOT = Path(__file__).resolve().parent
ENV_PATH = ROOT / ".env"

DEFAULT_ENV = {
    "MONGO_PORT": "27017",
    "MONGO_APP_DB": "milano2026",
    "MONGO_APP_USERNAME": "app_user",
    "MONGO_APP_PASSWORD": "app_password",
}


def load_env() -> dict[str, str]:
    values = DEFAULT_ENV.copy()
    if not ENV_PATH.exists():
        return values

    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def build_mongo_uri(env: dict[str, str]) -> str:
    return (
        f"mongodb://{env['MONGO_APP_USERNAME']}:{env['MONGO_APP_PASSWORD']}"
        f"@localhost:{env['MONGO_PORT']}/{env['MONGO_APP_DB']}"
        f"?authSource={env['MONGO_APP_DB']}"
    )


def get_database():
    env = load_env()
    client = MongoClient(build_mongo_uri(env), serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client, client[env["MONGO_APP_DB"]]


def run_scalar_aggregation(collection, pipeline: list[dict], field_name: str) -> int:
    result = list(collection.aggregate(pipeline))
    if not result:
        return 0
    return int(result[0].get(field_name, 0))


def run_list_aggregation(collection, pipeline: list[dict]) -> list[dict]:
    return list(collection.aggregate(pipeline))


def count_users(db) -> int:
    pipeline = [{"$count": "total_users"}]
    return run_scalar_aggregation(db["users"], pipeline, "total_users")


def count_tweets(db) -> int:
    pipeline = [{"$count": "total_tweets"}]
    return run_scalar_aggregation(db["tweets"], pipeline, "total_tweets")


def count_distinct_hashtags(db) -> int:
    pipeline = [
        {"$unwind": "$hashtags"},
        {"$group": {"_id": "$hashtags"}},
        {"$count": "total_distinct_hashtags"},
    ]
    return run_scalar_aggregation(db["tweets"], pipeline, "total_distinct_hashtags")


def count_tweets_with_hashtag(db, hashtag: str) -> int:
    pipeline = [
        {"$match": {"hashtags": hashtag}},
        {"$count": "total_tweets_with_hashtag"},
    ]
    return run_scalar_aggregation(db["tweets"], pipeline, "total_tweets_with_hashtag")


def count_distinct_users_with_hashtag(db, hashtag: str) -> int:
    pipeline = [
        {"$match": {"hashtags": hashtag}},
        {"$group": {"_id": "$user_id"}},
        {"$count": "total_distinct_users_with_hashtag"},
    ]
    return run_scalar_aggregation(
        db["tweets"], pipeline, "total_distinct_users_with_hashtag"
    )


def get_reply_tweets(db) -> list[dict]:
    pipeline = [
        {"$match": {"in_reply_to_tweet_id": {"$ne": None}}},
        {
            "$project": {
                "_id": 0,
                "tweet_id": 1,
                "user_id": 1,
                "in_reply_to_tweet_id": 1,
                "favorite_count": 1,
                "created_at": 1,
                "text": 1,
            }
        },
        {"$sort": {"created_at": 1, "tweet_id": 1}},
    ]
    return run_list_aggregation(db["tweets"], pipeline)


def get_top_tweets_by_popularity(db, limit: int = 10) -> list[dict]:
    pipeline = [
        {"$sort": {"favorite_count": -1, "tweet_id": 1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "tweet_id": 1,
                "user_id": 1,
                "text": 1,
                "favorite_count": 1,
                "created_at": 1,
            }
        },
    ]
    return run_list_aggregation(db["tweets"], pipeline)


def get_top_hashtags_by_popularity(db, limit: int = 10) -> list[dict]:
    pipeline = [
        {"$unwind": "$hashtags"},
        {"$group": {"_id": "$hashtags", "popularity": {"$sum": 1}}},
        {"$sort": {"popularity": -1, "_id": 1}},
        {"$limit": limit},
        {"$project": {"_id": 0, "hashtag": "$_id", "popularity": 1}},
    ]
    return run_list_aggregation(db["tweets"], pipeline)


def get_thread_starters(db) -> list[dict]:
    pipeline = [
        {"$match": {"in_reply_to_tweet_id": None}},
        {
            "$lookup": {
                "from": "tweets",
                "localField": "tweet_id",
                "foreignField": "in_reply_to_tweet_id",
                "as": "direct_replies",
            }
        },
        {"$match": {"direct_replies.0": {"$exists": True}}},
        {
            "$project": {
                "_id": 0,
                "tweet_id": 1,
                "user_id": 1,
                "favorite_count": 1,
                "created_at": 1,
                "text": 1,
                "direct_reply_count": {"$size": "$direct_replies"},
            }
        },
        {"$sort": {"created_at": 1, "tweet_id": 1}},
    ]
    return run_list_aggregation(db["tweets"], pipeline)


def get_longest_conversation(db) -> dict:
    # Ici, "la plus longue" est interpretee comme la conversation avec le plus de tweets.
    pipeline = [
        {"$match": {"in_reply_to_tweet_id": None}},
        {
            "$graphLookup": {
                "from": "tweets",
                "startWith": "$tweet_id",
                "connectFromField": "tweet_id",
                "connectToField": "in_reply_to_tweet_id",
                "as": "descendants",
                "depthField": "depth",
            }
        },
        {"$match": {"descendants.0": {"$exists": True}}},
        {
            "$addFields": {
                "conversation_size": {"$add": [1, {"$size": "$descendants"}]},
                "max_depth_value": {"$max": "$descendants.depth"},
            }
        },
        {
            "$addFields": {
                "longest_reply_chain_length": {"$add": ["$max_depth_value", 2]}
            }
        },
        {"$sort": {"conversation_size": -1, "longest_reply_chain_length": -1, "tweet_id": 1}},
        {"$limit": 1},
        {
            "$project": {
                "_id": 0,
                "start_tweet_id": "$tweet_id",
                "start_user_id": "$user_id",
                "start_text": "$text",
                "conversation_size": 1,
                "longest_reply_chain_length": 1,
                "descendant_tweet_ids": "$descendants.tweet_id",
            }
        },
    ]
    results = run_list_aggregation(db["tweets"], pipeline)
    return results[0] if results else {}


def get_conversation_boundaries(db) -> list[dict]:
    # Une conversation peut se terminer sur plusieurs tweets si le thread se ramifie.
    pipeline = [
        {"$match": {"in_reply_to_tweet_id": None}},
        {
            "$graphLookup": {
                "from": "tweets",
                "startWith": "$tweet_id",
                "connectFromField": "tweet_id",
                "connectToField": "in_reply_to_tweet_id",
                "as": "descendants",
                "depthField": "depth",
            }
        },
        {"$match": {"descendants.0": {"$exists": True}}},
        {"$addFields": {"reply_targets": "$descendants.in_reply_to_tweet_id"}},
        {
            "$addFields": {
                "ending_tweets": {
                    "$filter": {
                        "input": "$descendants",
                        "as": "tweet",
                        "cond": {
                            "$not": [{"$in": ["$$tweet.tweet_id", "$reply_targets"]}]
                        },
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "conversation_size": {"$add": [1, {"$size": "$descendants"}]},
                "start_tweet": {
                    "tweet_id": "$tweet_id",
                    "user_id": "$user_id",
                    "created_at": "$created_at",
                    "text": "$text",
                },
                "ending_tweets": {
                    "$map": {
                        "input": "$ending_tweets",
                        "as": "tweet",
                        "in": {
                            "tweet_id": "$$tweet.tweet_id",
                            "user_id": "$$tweet.user_id",
                            "created_at": "$$tweet.created_at",
                            "text": "$$tweet.text",
                        },
                    }
                },
            }
        },
        {"$sort": {"start_tweet.tweet_id": 1}},
    ]
    return run_list_aggregation(db["tweets"], pipeline)


def print_documents(title: str, documents: list[dict], preview: int | None = None) -> None:
    print(f"\n{title}")
    print("-" * len(title))
    print(f"Resultats : {len(documents)}")

    items = documents if preview is None else documents[:preview]
    for document in items:
        pprint(document)

    if preview is not None and len(documents) > preview:
        print(f"... {len(documents) - preview} autres resultats non affiches")


def main() -> None:
    client, db = get_database()

    try:
        example_hashtag = "shuttle"
        target_hashtag = "milano2026"

        print(f"Nombre d utilisateurs : {count_users(db)}")
        print(f"Nombre de tweets : {count_tweets(db)}")
        print(f"Nombre de hashtags distincts : {count_distinct_hashtags(db)}")
        print(
            f"Nombre de tweets contenant le hashtag '{example_hashtag}' : "
            f"{count_tweets_with_hashtag(db, example_hashtag)}"
        )
        print(
            f"Nombre d utilisateurs distincts ayant tweet avec '{target_hashtag}' : "
            f"{count_distinct_users_with_hashtag(db, target_hashtag)}"
        )

        print_documents(
            "Tweets qui sont des reponses a un autre tweet",
            get_reply_tweets(db),
            preview=10,
        )
        print_documents(
            "Top 10 des tweets les plus populaires",
            get_top_tweets_by_popularity(db, limit=10),
        )
        print_documents(
            "Top 10 des hashtags les plus populaires",
            get_top_hashtags_by_popularity(db, limit=10),
        )
        print_documents(
            "Tweets qui initient une discussion",
            get_thread_starters(db),
            preview=10,
        )

        print("\nDiscussion la plus longue")
        print("-------------------------")
        pprint(get_longest_conversation(db))

        print_documents(
            "Debut et fin de chaque conversation",
            get_conversation_boundaries(db),
            preview=10,
        )
    finally:
        client.close()


if __name__ == "__main__":
    main()
