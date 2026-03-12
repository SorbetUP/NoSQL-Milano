from pymongo import MongoClient

# Connexion à MongoDB
client = MongoClient("mongodb://localhost:27017")

# Base de données
db = client["milano2026"]

# Collections
users = db["users"]
tweets = db["tweets"]


# =========================
# CRUD USERS
# =========================

def insert_user(user_id, username, role, country, created_at):
    user = {
        "user_id": user_id,
        "username": username,
        "role": role,
        "country": country,
        "created_at": created_at
    }
    users.insert_one(user)
    print("User inserted")


def update_user(user_id, new_values):
    users.update_one(
        {"user_id": user_id},
        {"$set": new_values}
    )
    print("User updated")


def delete_user(user_id):
    users.delete_one({"user_id": user_id})
    print("User deleted")


# =========================
# CRUD TWEETS
# =========================

def insert_tweet(tweet_id, user_id, text, hashtags, created_at, favorite_count, in_reply_to_tweet_id=None):
    tweet = {
        "tweet_id": tweet_id,
        "user_id": user_id,
        "text": text,
        "hashtags": hashtags,
        "created_at": created_at,
        "favorite_count": favorite_count,
        "in_reply_to_tweet_id": in_reply_to_tweet_id
    }

    tweets.insert_one(tweet)
    print("Tweet inserted")


def update_tweet(tweet_id, new_values):
    tweets.update_one(
        {"tweet_id": tweet_id},
        {"$set": new_values}
    )
    print("Tweet updated")


def delete_tweet(tweet_id):
    tweets.delete_one({"tweet_id": tweet_id})
    print("Tweet deleted")


# =========================
# TEST DES FONCTIONS
# =========================

if __name__ == "__main__":


    # DELETE USER
    delete_user("kevin")