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
    cursor = conn.cursor()
    create_table_queries = [
        """
        CREATE TABLE IF NOT EXISTS Users (
            id SERIAL PRIMARY KEY,
            user_name TEXT,
            surname TEXT,
            email TEXT,
            password TEXT,
            isTrainer BOOLEAN,
            gender TEXT,
            weight INT,
            age FLOAT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS ExercisesName (
            id SERIAL PRIMARY KEY,
            EN_name TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Muscles (
            id SERIAL PRIMARY KEY,
            muscle_name TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Exercises (
            id SERIAL PRIMARY KEY,
            exercise_name_id INT REFERENCES ExercisesName(id),
            description TEXT,
            gif TEXT,
            tags TEXT[]
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS ExercisesMuscles (
            exercise_id INT REFERENCES Exercises(id),
            muscle_id INT REFERENCES Muscles(id),
            PRIMARY KEY (exercise_id, muscle_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Plans (
            id SERIAL PRIMARY KEY,
            weightsequences TEXT,
            date DATE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS ExercisesPlans (
            exercise_id INT REFERENCES Exercises(id),
            plan_id INT REFERENCES Plans(id),
            PRIMARY KEY (exercise_id, plan_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS UsersPlans (
            user_id INT REFERENCES Users(id),
            trainer_id INT REFERENCES Users(id),
            plan_id INT REFERENCES Plans(id),
            PRIMARY KEY (user_id, plan_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS TrainingExercises(
            id SERIAL PRIMARY KEY,
            exercise_id INT REFERENCES Exercises(id),
            sets INT,
            rep INT,
            weight FLOAT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Trainings (
            id SERIAL PRIMARY KEY,
            date DATE,
            user_id INT REFERENCES Users(id),
            training_exercise_id INT REFERENCES TrainingExercises(id),
            plan_id INT REFERENCES Plans(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS TTExercises (
            trainingexercise_id INT REFERENCES TrainingExercises(id),
            training_id INT REFERENCES Trainings(id),
            PRIMARY KEY (trainingexercise_id, training_id)
        );
        """
    ]

    for query in create_table_queries:
        cursor.execute(query)
        logger.info("Executed query: %s", query.strip())

    conn.commit()
    cursor.close()


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
