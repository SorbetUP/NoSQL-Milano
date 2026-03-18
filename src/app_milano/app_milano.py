import subprocess

from neo4j.exceptions import Neo4jError
from pymongo.errors import PyMongoError

from .config import load_settings
from .utils.display import print_connection_info, print_question_results
from .utils.docker import require_docker, start_services
from .utils.mongo import (
    count_distinct_hashtags,
    count_tweets_with_hashtag,
    count_tweets,
    count_users_who_tweeted_hashtag,
    count_users,
    get_conversation_boundaries,
    get_database,
    get_longest_conversation,
    get_reply_tweets,
    get_thread_starters,
    get_top_hashtags,
    get_top_tweets,
    import_dataset,
    load_dataset,
    wait_for_mongo,
)
from .utils.neo4j import (
    get_milanoops_followers,
    get_milanoops_following,
    get_mutual_connections_with_milanoops,
    get_users_following_more_than_five_users,
    get_users_with_more_than_ten_followers,
    import_graph,
    load_follows,
    wait_for_neo4j,
)


class AppMilano:
    def __init__(self) -> None:
        self.settings = load_settings()

    def run(self) -> None:
        try:
            self._run()
        except (PyMongoError, Neo4jError, subprocess.CalledProcessError) as exc:
            raise SystemExit(str(exc)) from exc

    def bootstrap(self):
        require_docker()
        start_services()

        mongo_client = wait_for_mongo(self.settings.mongo_root_uri)
        import_dataset(mongo_client, self.settings)
        mongo_db = get_database(mongo_client, self.settings)
        hashtag_spotlight = "milano2026"
        question_results = {
            "user_count": count_users(mongo_db),
            "tweet_count": count_tweets(mongo_db),
            "distinct_hashtag_count": count_distinct_hashtags(mongo_db),
            "hashtag_spotlight": hashtag_spotlight,
            "tweets_with_hashtag_count": count_tweets_with_hashtag(mongo_db, hashtag_spotlight),
            "distinct_users_with_hashtag_count": count_users_who_tweeted_hashtag(mongo_db, hashtag_spotlight),
            "reply_tweets": get_reply_tweets(mongo_db),
            "top_tweets": get_top_tweets(mongo_db),
            "top_hashtags": get_top_hashtags(mongo_db),
            "thread_starters": get_thread_starters(mongo_db),
            "longest_conversation": get_longest_conversation(mongo_db),
            "conversation_boundaries": get_conversation_boundaries(mongo_db),
        }
        users, _ = load_dataset()
        follows = load_follows()

        neo4j_driver = wait_for_neo4j(
            self.settings.neo4j_bolt_uri,
            self.settings.neo4j_user,
            self.settings.neo4j_password,
        )
        import_graph(neo4j_driver, users, follows)
        question_results.update(
            {
                "milanoops_followers": get_milanoops_followers(neo4j_driver),
                "milanoops_following": get_milanoops_following(neo4j_driver),
                "milanoops_mutual_connections": get_mutual_connections_with_milanoops(neo4j_driver),
                "users_with_more_than_ten_followers": get_users_with_more_than_ten_followers(neo4j_driver),
                "users_following_more_than_five_users": get_users_following_more_than_five_users(neo4j_driver),
            }
        )
        neo4j_driver.close()
        mongo_client.close()
        return question_results

    def _run(self) -> None:
        question_results = self.bootstrap()
        print_connection_info(self.settings)
        print_question_results(question_results)
