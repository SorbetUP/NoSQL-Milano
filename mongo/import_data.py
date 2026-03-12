import json
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["milano2026"]

users_collection = db["users"]
tweets_collection = db["tweets"]

# =========================
# IMPORT USERS
# =========================

with open("../data/users.json", "r", encoding="utf-8") as f:
    users = json.load(f)

users_collection.insert_many(users)

print("Users imported")


# =========================
# IMPORT TWEETS
# =========================

with open("../data/tweets.json", "r", encoding="utf-8") as f:
    tweets = json.load(f)

tweets_collection.insert_many(tweets)

print("Tweets imported")