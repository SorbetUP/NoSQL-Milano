import json
import re
import time
from collections import defaultdict

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.database import Database
from pymongo.errors import PyMongoError

from app_milano.config import DATA_DIR, Settings, load_env_file, load_settings


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
    return list(
        db.tweets.aggregate(
            [
                {"$match": {"in_reply_to_tweet_id": {"$ne": None}}},
                {"$sort": {"created_at": -1, "tweet_id": 1}},
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
                        "text": 1,
                        "hashtags": 1,
                        "created_at": 1,
                        "favorite_count": 1,
                        "in_reply_to_tweet_id": 1,
                        "username": {"$arrayElemAt": ["$author.username", 0]},
                    }
                },
            ]
        )
    )

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


# Aggregation Mongo: tweets qui initient une discussion
def get_thread_starters(db: Database) -> list[dict]:
    return list(
        db.tweets.aggregate(
            [
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
        )
    )


# Aggregation Mongo: discussion la plus longue
def get_longest_conversation(db: Database) -> dict:
    rows = list(
        db.tweets.aggregate(
            [
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
                {
                    "$sort": {
                        "conversation_size": -1,
                        "longest_reply_chain_length": -1,
                        "tweet_id": 1,
                    }
                },
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
        )
    )
    return rows[0] if rows else {}


# Aggregation Mongo: debut et fin de chaque conversation
def get_conversation_boundaries(db: Database) -> list[dict]:
    return list(
        db.tweets.aggregate(
            [
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
        )
    )


def _connect_mongo_context(context):
    if not load_env_file(required=False):
        return

    try:
        context["settings"] = load_settings()
        context["client"] = MongoClient(context["settings"].mongo_app_uri, serverSelectionTimeoutMS=2500)
        context["client"].admin.command("ping")
        context["db"] = context["client"][context["settings"].mongo_app_db]
        context["source"] = "MongoDB"
    except Exception:
        if context["client"]:
            context["client"].close()
        context["client"] = None
        context["db"] = None
        context["settings"] = None
        context["source"] = "JSON local"


def _clean_doc(doc):
    if not doc:
        return None
    cleaned = dict(doc)
    cleaned.pop("_id", None)
    return cleaned


def _normalize_hashtag(hashtag):
    return hashtag.strip().lstrip("#").lower()


def _attach_username(context, tweet):
    if not tweet:
        return None
    enriched = dict(tweet)
    if not enriched.get("username"):
        user = context["users_by_id"].get(enriched.get("user_id"), {})
        enriched["username"] = user.get("username", "unknown")
    return enriched


def _sort_by_created_at(rows, reverse=False):
    return sorted(rows, key=lambda item: (item.get("created_at", ""), item.get("tweet_id", "")), reverse=reverse)


def _get_conversation_boundaries_json(context):
    children_by_parent = defaultdict(list)
    tweets_by_id = {}

    for tweet in context["tweets"]:
        tweet_id = tweet.get("tweet_id")
        if tweet_id:
            tweets_by_id[tweet_id] = _attach_username(context, tweet)
        parent_id = tweet.get("in_reply_to_tweet_id")
        if parent_id:
            children_by_parent[parent_id].append(tweet_id)

    boundaries = []
    for tweet in context["tweets"]:
        if tweet.get("in_reply_to_tweet_id") is not None:
            continue

        start = _attach_username(context, tweet)
        descendants = []
        ending_tweets = []
        max_depth = 0
        stack = [(tweet.get("tweet_id"), 0)]

        while stack:
            parent_id, depth = stack.pop()
            children_ids = children_by_parent.get(parent_id, [])
            if not children_ids and depth > 0:
                leaf = tweets_by_id.get(parent_id)
                if leaf:
                    ending_tweets.append(leaf)
            for child_id in children_ids:
                child = tweets_by_id.get(child_id)
                if not child:
                    continue
                descendants.append(child)
                child_depth = depth + 1
                if child_depth > max_depth:
                    max_depth = child_depth
                stack.append((child_id, child_depth))

        if not descendants:
            continue

        boundaries.append(
            {
                "conversation_size": 1 + len(descendants),
                "longest_reply_chain_length": max_depth + 1,
                "start_tweet": {
                    "tweet_id": start.get("tweet_id"),
                    "user_id": start.get("user_id"),
                    "username": start.get("username"),
                    "created_at": start.get("created_at"),
                    "text": start.get("text"),
                    "hashtags": start.get("hashtags", []),
                    "favorite_count": start.get("favorite_count", 0),
                    "in_reply_to_tweet_id": start.get("in_reply_to_tweet_id"),
                },
                "ending_tweets": _sort_by_created_at(ending_tweets),
            }
        )

    return sorted(boundaries, key=lambda item: item["start_tweet"].get("tweet_id", ""))


def create_mongo_context(placeholder="in progress"):
    users, tweets = load_dataset()
    context = {
        "placeholder": placeholder,
        "source": "JSON local",
        "client": None,
        "db": None,
        "settings": None,
        "users": users,
        "tweets": tweets,
        "users_by_id": {user["user_id"]: user for user in users},
        "users_by_username": {user["username"].lower(): user for user in users},
    }
    _connect_mongo_context(context)
    return context


def close_mongo_context(context):
    if context and context["client"]:
        context["client"].close()


def get_mongo_source(context):
    return context["source"]


def get_ui_kpis(context):
    if context["db"] is not None:
        return {
            "user_count": count_users(context["db"]),
            "tweet_count": count_tweets(context["db"]),
            "distinct_hashtag_count": count_distinct_hashtags(context["db"]),
        }

    hashtags = set()
    for tweet in context["tweets"]:
        for hashtag in tweet.get("hashtags", []):
            hashtags.add(hashtag)

    return {
        "user_count": len(context["users"]),
        "tweet_count": len(context["tweets"]),
        "distinct_hashtag_count": len(hashtags),
    }


def get_ui_top_tweets(context):
    if context["db"] is not None:
        return get_top_tweets(context["db"])

    tweets = []
    for tweet in _sort_by_created_at(context["tweets"], reverse=True):
        tweets.append(_attach_username(context, tweet))
    tweets = sorted(tweets, key=lambda item: (-item.get("favorite_count", 0), item.get("tweet_id", "")))
    return tweets[:10]


def get_ui_top_hashtags(context):
    if context["db"] is not None:
        return get_top_hashtags(context["db"])

    counts = {}
    for tweet in context["tweets"]:
        for hashtag in tweet.get("hashtags", []):
            counts[hashtag] = counts.get(hashtag, 0) + 1

    rows = []
    for hashtag, total in counts.items():
        rows.append({"hashtag": hashtag, "tweet_count": total})
    return sorted(rows, key=lambda item: (-item["tweet_count"], item["hashtag"]))[:10]


def get_ui_activity_series(context, limit=7):
    if context["db"] is not None:
        rows = list(
            context["db"].tweets.aggregate(
                [
                    {"$group": {"_id": {"$substr": ["$created_at", 0, 10]}, "tweet_count": {"$sum": 1}}},
                    {"$sort": {"_id": 1}},
                ]
            )
        )
        rows = [{"day": row["_id"], "tweet_count": row["tweet_count"]} for row in rows]
    else:
        counts = defaultdict(int)
        for tweet in context["tweets"]:
            counts[tweet.get("created_at", "")[:10]] += 1
        rows = [{"day": day, "tweet_count": count} for day, count in sorted(counts.items())]
    return rows[-limit:]


def search_ui_users(context, query, limit=12):
    query = query.strip()
    if not query:
        return []

    if context["db"] is not None:
        cursor = (
            context["db"].users.find({"username": {"$regex": re.escape(query), "$options": "i"}}, {"_id": 0})
            .sort("username", 1)
            .limit(limit)
        )
        return [_clean_doc(row) for row in cursor]

    results = []
    needle = query.lower()
    for user in context["users"]:
        if needle in user.get("username", "").lower():
            results.append(dict(user))
    return sorted(results, key=lambda item: item["username"])[:limit]


def search_ui_hashtags(context, query, limit=12):
    query = _normalize_hashtag(query)
    if not query:
        return []

    if context["db"] is not None:
        return list(
            context["db"].tweets.aggregate(
                [
                    {"$unwind": "$hashtags"},
                    {"$match": {"hashtags": {"$regex": re.escape(query), "$options": "i"}}},
                    {"$group": {"_id": "$hashtags", "tweet_count": {"$sum": 1}}},
                    {"$sort": {"tweet_count": -1, "_id": 1}},
                    {"$limit": limit},
                    {"$project": {"_id": 0, "hashtag": "$_id", "tweet_count": 1}},
                ]
            )
        )

    counts = {}
    for tweet in context["tweets"]:
        for hashtag in tweet.get("hashtags", []):
            if query in hashtag.lower():
                counts[hashtag] = counts.get(hashtag, 0) + 1

    rows = []
    for hashtag, total in counts.items():
        rows.append({"hashtag": hashtag, "tweet_count": total})
    return sorted(rows, key=lambda item: (-item["tweet_count"], item["hashtag"]))[:limit]


def search_ui_tweets_by_text(context, query, limit=20):
    query = query.strip()
    if not query:
        return []

    if context["db"] is not None:
        return list(
            context["db"].tweets.aggregate(
                [
                    {"$match": {"text": {"$regex": re.escape(query), "$options": "i"}}},
                    {"$sort": {"created_at": -1, "tweet_id": 1}},
                    {"$limit": limit},
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
                            "text": 1,
                            "hashtags": 1,
                            "created_at": 1,
                            "favorite_count": 1,
                            "in_reply_to_tweet_id": 1,
                            "username": {"$arrayElemAt": ["$author.username", 0]},
                        }
                    },
                ]
            )
        )

    needle = query.lower()
    rows = []
    for tweet in context["tweets"]:
        if needle in tweet.get("text", "").lower():
            rows.append(_attach_username(context, tweet))
    return _sort_by_created_at(rows, reverse=True)[:limit]


def get_ui_user_by_username(context, username):
    if not username:
        return None

    if context["db"] is not None:
        row = context["db"].users.find_one(
            {"username": {"$regex": f"^{re.escape(username)}$", "$options": "i"}},
            {"_id": 0},
        )
        return _clean_doc(row)

    return context["users_by_username"].get(username.lower())


def get_ui_user_by_id(context, user_id):
    if not user_id:
        return None

    if context["db"] is not None:
        row = context["db"].users.find_one({"user_id": user_id}, {"_id": 0})
        return _clean_doc(row)

    return context["users_by_id"].get(user_id)


def get_ui_tweets_by_user(context, user_id, limit=20):
    if not user_id:
        return []

    if context["db"] is not None:
        cursor = context["db"].tweets.find({"user_id": user_id}, {"_id": 0}).sort("created_at", -1).limit(limit)
        rows = [_clean_doc(row) for row in cursor]
        return [_attach_username(context, row) for row in rows]

    rows = [_attach_username(context, tweet) for tweet in context["tweets"] if tweet.get("user_id") == user_id]
    return _sort_by_created_at(rows, reverse=True)[:limit]


def get_ui_hashtag_summary(context, hashtag):
    normalized = _normalize_hashtag(hashtag)
    if not normalized:
        return None

    if context["db"] is not None:
        rows = list(
            context["db"].tweets.aggregate(
                [
                    {"$match": {"hashtags": normalized}},
                    {"$group": {"_id": None, "tweet_count": {"$sum": 1}, "user_ids": {"$addToSet": "$user_id"}}},
                    {"$project": {"_id": 0, "hashtag": normalized, "tweet_count": 1, "user_count": {"$size": "$user_ids"}}},
                ]
            )
        )
        return rows[0] if rows else None

    matching = [tweet for tweet in context["tweets"] if normalized in tweet.get("hashtags", [])]
    if not matching:
        return None

    user_ids = set()
    for tweet in matching:
        user_ids.add(tweet.get("user_id"))
    return {"hashtag": normalized, "tweet_count": len(matching), "user_count": len(user_ids)}


def get_ui_tweets_by_hashtag(context, hashtag, limit=30):
    normalized = _normalize_hashtag(hashtag)
    if not normalized:
        return []

    if context["db"] is not None:
        return list(
            context["db"].tweets.aggregate(
                [
                    {"$match": {"hashtags": normalized}},
                    {"$sort": {"created_at": -1, "tweet_id": 1}},
                    {"$limit": limit},
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
                            "text": 1,
                            "hashtags": 1,
                            "created_at": 1,
                            "favorite_count": 1,
                            "in_reply_to_tweet_id": 1,
                            "username": {"$arrayElemAt": ["$author.username", 0]},
                        }
                    },
                ]
            )
        )

    rows = []
    for tweet in context["tweets"]:
        if normalized in tweet.get("hashtags", []):
            rows.append(_attach_username(context, tweet))
    return _sort_by_created_at(rows, reverse=True)[:limit]


def get_ui_tweet_by_id(context, tweet_id):
    if not tweet_id:
        return None

    if context["db"] is not None:
        row = context["db"].tweets.find_one({"tweet_id": tweet_id}, {"_id": 0})
        return _attach_username(context, _clean_doc(row))

    for tweet in context["tweets"]:
        if tweet.get("tweet_id") == tweet_id:
            return _attach_username(context, tweet)
    return None


def get_ui_parent_tweet(context, tweet):
    if not tweet:
        return None
    return get_ui_tweet_by_id(context, tweet.get("in_reply_to_tweet_id"))


def get_ui_reply_tweets(context, limit=60):
    if context["db"] is not None:
        rows = get_reply_tweets(context["db"])
        if limit:
            return rows[:limit]
        return rows

    rows = []
    for tweet in context["tweets"]:
        if tweet.get("in_reply_to_tweet_id") is not None:
            rows.append(_attach_username(context, tweet))
    rows = _sort_by_created_at(rows, reverse=True)
    if limit:
        return rows[:limit]
    return rows


def get_ui_replies_for_tweet(context, tweet_id, limit=30):
    if not tweet_id:
        return []

    if context["db"] is not None:
        return list(
            context["db"].tweets.aggregate(
                [
                    {"$match": {"in_reply_to_tweet_id": tweet_id}},
                    {"$sort": {"created_at": 1, "tweet_id": 1}},
                    {"$limit": limit},
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
                            "text": 1,
                            "hashtags": 1,
                            "created_at": 1,
                            "favorite_count": 1,
                            "in_reply_to_tweet_id": 1,
                            "username": {"$arrayElemAt": ["$author.username", 0]},
                        }
                    },
                ]
            )
        )

    rows = []
    for tweet in context["tweets"]:
        if tweet.get("in_reply_to_tweet_id") == tweet_id:
            rows.append(_attach_username(context, tweet))
    return _sort_by_created_at(rows)[:limit]


def get_ui_extended_conversation(context, tweet_id):
    tweet = get_ui_tweet_by_id(context, tweet_id)
    if not tweet:
        return None

    root = tweet
    visited = set()
    while root and root.get("in_reply_to_tweet_id"):
        root_id = root.get("tweet_id")
        if root_id in visited:
            break
        visited.add(root_id)
        parent = get_ui_parent_tweet(context, root)
        if not parent:
            break
        root = parent

    if not root:
        return None

    if context["db"] is not None:
        boundaries = get_conversation_boundaries(context["db"])
        longest = get_longest_conversation(context["db"])
        starters = get_thread_starters(context["db"])

        selected = None
        for item in boundaries:
            start_tweet = item.get("start_tweet", {})
            if start_tweet.get("tweet_id") == root.get("tweet_id"):
                selected = dict(item)
                break

        starter = None
        for item in starters:
            if item.get("tweet_id") == root.get("tweet_id"):
                starter = dict(item)
                break

        if selected:
            selected["start_tweet"] = _attach_username(context, selected.get("start_tweet"))
            selected["ending_tweets"] = [_attach_username(context, item) for item in selected.get("ending_tweets", [])]
        else:
            selected = {
                "conversation_size": 1,
                "longest_reply_chain_length": 1,
                "start_tweet": _attach_username(context, root),
                "ending_tweets": [],
            }

        selected["root_tweet_id"] = root.get("tweet_id")
        selected["is_longest"] = longest.get("start_tweet_id") == root.get("tweet_id")
        selected["direct_reply_count"] = starter.get("direct_reply_count", 0) if starter else 0
        return selected

    boundaries = _get_conversation_boundaries_json(context)
    selected = None
    for item in boundaries:
        start_tweet = item.get("start_tweet", {})
        if start_tweet.get("tweet_id") == root.get("tweet_id"):
            selected = dict(item)
            break

    if not selected:
        selected = {
            "conversation_size": 1,
            "longest_reply_chain_length": 1,
            "start_tweet": _attach_username(context, root),
            "ending_tweets": [],
        }

    longest_size = 0
    longest_chain = 0
    longest_root_id = ""
    for item in boundaries:
        size = item.get("conversation_size", 0)
        chain = item.get("longest_reply_chain_length", 0)
        if size > longest_size or (size == longest_size and chain > longest_chain):
            longest_size = size
            longest_chain = chain
            longest_root_id = item.get("start_tweet", {}).get("tweet_id", "")

    selected["root_tweet_id"] = root.get("tweet_id")
    selected["is_longest"] = longest_root_id == root.get("tweet_id")
    selected["direct_reply_count"] = len(get_ui_replies_for_tweet(context, root.get("tweet_id", ""), limit=500))
    return selected


def get_ui_longest_conversation_summary(context):
    if context["db"] is not None:
        row = get_longest_conversation(context["db"])
        if not row:
            return None
        start_tweet = get_ui_tweet_by_id(context, row.get("start_tweet_id", ""))
        return {
            "start_tweet": start_tweet,
            "conversation_size": row.get("conversation_size", context["placeholder"]),
            "longest_reply_chain_length": row.get("longest_reply_chain_length", context["placeholder"]),
            "ending_tweet_count": len(row.get("descendant_tweet_ids", [])),
        }

    boundaries = _get_conversation_boundaries_json(context)
    if not boundaries:
        return None

    best = None
    for item in boundaries:
        if best is None:
            best = item
            continue
        size = item.get("conversation_size", 0)
        best_size = best.get("conversation_size", 0)
        chain = item.get("longest_reply_chain_length", 0)
        best_chain = best.get("longest_reply_chain_length", 0)
        if size > best_size or (size == best_size and chain > best_chain):
            best = item

    if not best:
        return None

    return {
        "start_tweet": _attach_username(context, best.get("start_tweet")),
        "conversation_size": best.get("conversation_size", context["placeholder"]),
        "longest_reply_chain_length": best.get("longest_reply_chain_length", context["placeholder"]),
        "ending_tweet_count": len(best.get("ending_tweets", [])),
    }
