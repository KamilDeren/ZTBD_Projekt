import psycopg2
from psycopg2 import sql
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
import uuid
import logging
from pymongo import MongoClient

#TODO zmienić inserty dla każdej bazy, nwm na razie jak dane chcemy wrzucać, mamy tutaj szablon

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Konfiguracja bazy danych Cassandra
KEYSPACE_NAME = "trainings_ztbd"
CASSANDRA_CONTACT_POINTS = ["127.0.0.1"]
CASSANDRA_PORT = 9042
USERNAME = "admin"
PASSWORD = "password"

# Konfiguracja bazy danych PostgreSQL
POSTGRES_HOST = "127.0.0.1"
POSTGRES_PORT = 5432
POSTGRES_DB = "trainings_ztbd"
POSTGRES_USER = "admin"
POSTGRES_PASSWORD = "password"

# Konfiguracja bazy danych MongoDB
MONGO_HOST = "127.0.0.1"
MONGO_PORT = 27017
MONGO_DB = "trainings_ztbd"
MONGO_COLLECTION = "users"


def get_cassandra_session():
    auth_provider = PlainTextAuthProvider(username=USERNAME, password=PASSWORD)
    cluster = Cluster(contact_points=CASSANDRA_CONTACT_POINTS, port=CASSANDRA_PORT, auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace(KEYSPACE_NAME)
    return session


def insert_data_into_cassandra(session, num_rows):
    insert_query = session.prepare(
        "INSERT INTO users (user_id, name, surname, email, pass, isTrainer, gender, weight, age) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")

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


def get_postgres_connection():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    return conn


def insert_data_into_postgres(conn, num_rows):
    cursor = conn.cursor()

    for i in range(num_rows):
        name = f"Name {i}"
        surname = f"Surname {i}"
        email = f"email{i}@example.com"
        password = "password"
        is_trainer = i % 2 == 0
        gender = f"Gender {i}"
        weight = 70.0 + i
        age = 20 + i

        insert_query = sql.SQL("INSERT INTO users (name, surname, email, pass, isTrainer, gender, weight, age) "
                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")

        cursor.execute(insert_query, (name, surname, email, password, is_trainer, gender, weight, age))

    conn.commit()
    cursor.close()


def get_mongo_client():
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    return client


def insert_data_into_mongodb(client, num_rows):
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    for i in range(num_rows):
        name = f"Name {i}"
        surname = f"Surname {i}"
        email = f"email{i}@example.com"
        password = "password"
        is_trainer = i % 2 == 0
        gender = f"Gender {i}"
        weight = 70.0 + i
        age = 20 + i

        user_data = {
            "name": name,
            "surname": surname,
            "email": email,
            "password": password,
            "isTrainer": is_trainer,
            "gender": gender,
            "weight": weight,
            "age": age
        }

        collection.insert_one(user_data)


def main():
    # Połączenie z bazą danych Cassandra
    cassandra_session = get_cassandra_session()

    start_time_cassandra = datetime.now()

    # Wstawianie danych do Cassandra
    insert_data_into_cassandra(cassandra_session, 200000)

    end_time_cassandra = datetime.now()

    duration_cassandra = end_time_cassandra - start_time_cassandra

    logger.info("Czas wstawiania danych do bazy Cassandra: %s", duration_cassandra)

    # Zakończ połączenie z Cassandra
    cassandra_session.shutdown()

    # Połączenie z bazą danych PostgreSQL
    postgres_conn = get_postgres_connection()

    start_time_postgres = datetime.now()

    # Wstawianie danych do PostgreSQL
    insert_data_into_postgres(postgres_conn, 200000)

    end_time_postgres = datetime.now()

    duration_postgres = end_time_postgres - start_time_postgres

    logger.info("Czas wstawiania danych do bazy PostgreSQL: %s", duration_postgres)

    # Zakończ połączenie z PostgreSQL
    postgres_conn.close()

    # Połączenie z bazą danych MongoDB
    mongo_client = get_mongo_client()

    start_time_mongodb = datetime.now()

    # Wstawianie danych do MongoDB
    insert_data_into_mongodb(mongo_client, 200000)

    end_time_mongodb = datetime.now()

    duration_mongodb = end_time_mongodb - start_time_mongodb

    logger.info("Czas wstawiania danych do bazy MongoDB: %s", duration_mongodb)

    # Zakończ połączenie z MongoDB
    mongo_client.close()


if __name__ == "__main__":
    main()
