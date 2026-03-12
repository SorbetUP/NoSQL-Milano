import json
from pathlib import Path

from pymongo import MongoClient


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

client = MongoClient("mongodb://localhost:27017")
db = client["milano2026"]

users_collection = db["users"]
tweets_collection = db["tweets"]

# =========================
# IMPORT USERS
# =========================

with open(DATA_DIR / "users.json", "r", encoding="utf-8") as f:
    users = json.load(f)

users_collection.insert_many(users)

print("Users imported")


# =========================
# IMPORT TWEETS
# =========================

with open(DATA_DIR / "tweets.json", "r", encoding="utf-8") as f:
    tweets = json.load(f)

tweets_collection.insert_many(tweets)

print("Tweets imported")
