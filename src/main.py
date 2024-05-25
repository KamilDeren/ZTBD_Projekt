import logging
from cassandra_insert import main as cassandra_main
from postgres_insert import main as postgres_main
from mongodb_insert import main as mongodb_main

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    logger.info("Starting Cassandra data insertion")
    cassandra_main()
    logger.info("Finished Cassandra data insertion")

    logger.info("Starting PostgreSQL data insertion")
    postgres_main()
    logger.info("Finished PostgreSQL data insertion")

    logger.info("Starting MongoDB data insertion")
    mongodb_main()
    logger.info("Finished MongoDB data insertion")


if __name__ == "__main__":
    main()
