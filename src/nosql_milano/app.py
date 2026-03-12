from __future__ import annotations

import subprocess

from neo4j.exceptions import Neo4jError
from pymongo.errors import PyMongoError

from .config import load_settings
from .docker_utils import require_docker, start_services
from .mongo_service import import_dataset, load_dataset, run_questions, wait_for_mongo
from .neo4j_service import import_graph, wait_for_neo4j
from .reporting import print_connection_info, print_question_results


def main() -> None:
    settings = load_settings()
    require_docker()
    start_services()

    mongo_client = wait_for_mongo(settings.mongo_root_uri)
    import_dataset(mongo_client, settings)
    question_results = run_questions(mongo_client, settings)
    users, tweets = load_dataset()

    neo4j_driver = wait_for_neo4j(
        settings.neo4j_bolt_uri,
        settings.neo4j_user,
        settings.neo4j_password,
    )
    import_graph(neo4j_driver, users, tweets)
    neo4j_driver.close()
    mongo_client.close()

    print_connection_info(settings)
    print_question_results(question_results)


def run_cli() -> None:
    try:
        main()
    except (PyMongoError, Neo4jError, subprocess.CalledProcessError) as exc:
        raise SystemExit(str(exc)) from exc
