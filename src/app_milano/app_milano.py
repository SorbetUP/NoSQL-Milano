import subprocess

from neo4j.exceptions import Neo4jError
from pymongo.errors import PyMongoError

from .config import load_settings
from .utils.display import print_connection_info, print_question_results
from .utils.docker import require_docker, start_services
from .utils.mongo import (
    count_distinct_hashtags,
    count_tweets,
    count_users,
    get_database,
    get_top_hashtags,
    get_top_tweets,
    import_dataset,
    load_dataset,
    wait_for_mongo,
)
from .utils.neo4j import import_graph, wait_for_neo4j


class AppMilano:
    def __init__(self) -> None:
        self.settings = load_settings()

    def run(self) -> None:
        try:
            self._run()
        except (PyMongoError, Neo4jError, subprocess.CalledProcessError) as exc:
            raise SystemExit(str(exc)) from exc

    def _run(self) -> None:
        require_docker()
        start_services()

        mongo_client = wait_for_mongo(self.settings.mongo_root_uri)
        import_dataset(mongo_client, self.settings)
        mongo_db = get_database(mongo_client, self.settings)
        question_results = {
            "user_count": count_users(mongo_db),
            "tweet_count": count_tweets(mongo_db),
            "distinct_hashtag_count": count_distinct_hashtags(mongo_db),
            "top_tweets": get_top_tweets(mongo_db),
            "top_hashtags": get_top_hashtags(mongo_db),
        }
        users, tweets = load_dataset()

        neo4j_driver = wait_for_neo4j(
            self.settings.neo4j_bolt_uri,
            self.settings.neo4j_user,
            self.settings.neo4j_password,
        )
        import_graph(neo4j_driver, users, tweets)
        neo4j_driver.close()
        mongo_client.close()

        print_connection_info(self.settings)
        print_question_results(question_results)
