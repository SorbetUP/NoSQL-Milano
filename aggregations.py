from __future__ import annotations

from pathlib import Path

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


def count_users(db) -> int:
    pipeline = [{"$count": "total_users"}]
    return run_scalar_aggregation(db["users"], pipeline, "total_users")


def count_tweets(db) -> int:
    pipeline = [{"$count": "total_tweets"}]
    return run_scalar_aggregation(db["tweets"], pipeline, "total_tweets")


def count_distinct_hashtags(db) -> int:
    # hashtags est un tableau, donc il faut le deplier avant de compter les valeurs distinctes
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




def main() -> None:
    client, db = get_database()

    try:
        example_hashtag = "shuttle"

        print(f"Nombre d utilisateurs : {count_users(db)}")
        print(f"Nombre de tweets : {count_tweets(db)}")
        print(f"Nombre de hashtags distincts : {count_distinct_hashtags(db)}")
        print(
            f"Nombre de tweets contenant le hashtag '{example_hashtag}' : "
            f"{count_tweets_with_hashtag(db, example_hashtag)}"
        )
    finally:
        client.close()


if __name__ == "__main__":
    main()
