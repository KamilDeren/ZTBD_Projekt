import logging
from cassandra_insert import main as cassandra_main
from postgres_insert import main as postgres_main
from mongodb_insert import main as mongodb_main

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    #TODO: Jak będziesz to puszczał postgresa to puść tego initial_inserta najpierw dla jakiejś małej liczby danych i dopiero potem mierz
    # czas dla tych dwóch insertów co tam są w tym mierzeniu czasu, bo tu wszystkie tabele są inicjowane bo inaczej błąd wywalało


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
