import uuid
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Konfiguracja bazy danych Cassandra
KEYSPACE_NAME = "trainings_ztbd"
CASSANDRA_CONTACT_POINTS = ["127.0.0.1"]
CASSANDRA_PORT = 9042
USERNAME = "admin"
PASSWORD = "password"


def get_cassandra_session():
    auth_provider = PlainTextAuthProvider(username=USERNAME, password=PASSWORD)
    cluster = Cluster(contact_points=CASSANDRA_CONTACT_POINTS, port=CASSANDRA_PORT, auth_provider=auth_provider)
    session = cluster.connect()
    create_keyspace_query = f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE_NAME}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }}
        """
    session.execute(create_keyspace_query)
    session.set_keyspace(KEYSPACE_NAME)
    return session


def create_tables_cassandra(session):
    # TODO Stworzenie tabeli
    pass


def insert_data_into_cassandra(session, num_rows):
    # TODO Zrobić inserty na podstawie stworzonych tabeli;
    insert_query = session.prepare(
        "INSERT INTO users (user_id, name, surname, email, pass, isTrainer, gender, weight, age) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )

    for i in range(num_rows):
        user_id = uuid.uuid4()
        name = f"Name {i}"
        surname = f"Surname {i}"
        email = f"email{i}@example.com"
        password = "password"
        is_trainer = i % 2 == 0
        gender = f"Gender {i}"
        weight = 70.0 + i
        age = 20 + i

        session.execute(insert_query.bind((user_id, name, surname, email, password, is_trainer, gender, weight, age)))


def main():
    cassandra_session = get_cassandra_session()

    start_time_cassandra = datetime.now()
    insert_data_into_cassandra(cassandra_session, 200000)
    end_time_cassandra = datetime.now()

    duration_cassandra = end_time_cassandra - start_time_cassandra
    logger.info("Czas wstawiania danych do bazy Cassandra: %s", duration_cassandra)

    cassandra_session.shutdown()


if __name__ == "__main__":
    main()
