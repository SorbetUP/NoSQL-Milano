import json
import time

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.database import Database
from pymongo.errors import PyMongoError

from app_milano.config import DATA_DIR, Settings


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


def load_dataset() -> tuple[list[dict], list[dict]]:
    users = json.loads((DATA_DIR / "users.json").read_text(encoding="utf-8"))
    tweets = json.loads((DATA_DIR / "tweets.json").read_text(encoding="utf-8"))
    return users, tweets


def ensure_app_user(client: MongoClient, settings: Settings) -> None:
    app_db = client[settings.mongo_app_db]
    user_info = app_db.command("usersInfo", settings.mongo_app_username)
    command = "updateUser" if user_info.get("users") else "createUser"
    app_db.command(
        command,
        settings.mongo_app_username,
        pwd=settings.mongo_app_password,
        roles=[{"role": "readWrite", "db": settings.mongo_app_db}],
    )


def ensure_indexes(db: Database) -> None:
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


def import_dataset(client: MongoClient, settings: Settings) -> None:
    ensure_app_user(client, settings)
    db = client[settings.mongo_app_db]
    users, tweets = load_dataset()

    db.users.delete_many({})
    db.tweets.delete_many({})

    if users:
        db.users.insert_many(users, ordered=True)
    if tweets:
        db.tweets.insert_many(tweets, ordered=True)

    ensure_indexes(db)


def get_database(client: MongoClient, settings: Settings) -> Database:
    return client[settings.mongo_app_db]


# Question 1: nombre total d'utilisateurs
def count_users(db: Database) -> int:
    return db.users.count_documents({})


# Question 2: nombre total de tweets
def count_tweets(db: Database) -> int:
    return db.tweets.count_documents({})


# Question 3: nombre total de hashtags distincts
def count_distinct_hashtags(db: Database) -> int:
    result = list(
        db.tweets.aggregate(
            [
                {"$unwind": "$hashtags"},
                {"$group": {"_id": "$hashtags"}},
                {"$count": "count"},
            ]
        )
    )
    return result[0]["count"] if result else 0

# Question 4: nombre de tweets contenant un hashtag donne
def count_tweets_with_hashtag(db: Database, hashtag: str) -> int:
    pass


# Question 5: nombre d'utilisateurs distincts ayant tweeté avec un hashtag donne
def count_users_who_tweeted_hashtag(db: Database, hashtag: str) -> int:
    pass


# Question 6: tweets qui sont des reponses a un autre tweet
def get_reply_tweets(db: Database) -> list[dict]:
    pass

# Question 12: top 10 tweets les plus populaires
def get_top_tweets(db: Database) -> list[dict]:
    return list(
        db.tweets.aggregate(
            [
                {"$sort": {"favorite_count": -1, "tweet_id": 1}},
                {"$limit": 10},
                {
                    "$lookup": {
                        "from": "users",
                        "localField": "user_id",
                        "foreignField": "user_id",
                        "as": "author",
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "tweet_id": 1,
                        "user_id": 1,
                        "favorite_count": 1,
                        "text": 1,
                        "hashtags": 1,
                        "created_at": 1,
                        "in_reply_to_tweet_id": 1,
                        "username": {"$arrayElemAt": ["$author.username", 0]},
                    }
                },
            ]
        )
    )


# Question 13: top 10 hashtags les plus populaires
def get_top_hashtags(db: Database) -> list[dict]:
    return list(
        db.tweets.aggregate(
            [
                {"$unwind": "$hashtags"},
                {"$group": {"_id": "$hashtags", "tweet_count": {"$sum": 1}}},
                {"$sort": {"tweet_count": -1, "_id": 1}},
                {"$limit": 10},
                {"$project": {"_id": 0, "hashtag": "$_id", "tweet_count": 1}},
            ]
        )
    )
