import psycopg2
from psycopg2 import sql
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Konfiguracja bazy danych PostgreSQL
POSTGRES_HOST = "127.0.0.1"
POSTGRES_PORT = 5432
POSTGRES_DB = "trainings_ztbd"
POSTGRES_USER = "admin"
POSTGRES_PASSWORD = "password"

def get_postgres_connection():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    return conn


def create_tables_postgres(conn):
    # TODO Stworzenie tabeli
    pass


def insert_data_into_postgres(conn, num_rows):
    # TODO Zrobić inserty na podstawie stworzonych tabeli;
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

def main():
    postgres_conn = get_postgres_connection()

    start_time_postgres = datetime.now()
    insert_data_into_postgres(postgres_conn, 200000)
    end_time_postgres = datetime.now()

    duration_postgres = end_time_postgres - start_time_postgres
    logger.info("Czas wstawiania danych do bazy PostgreSQL: %s", duration_postgres)

    postgres_conn.close()

if __name__ == "__main__":
    main()
