import random

import psycopg2
from psycopg2 import sql
from datetime import datetime, date
import logging
import faker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Konfiguracja bazy danych PostgreSQL
POSTGRES_HOST = "127.0.0.1"
POSTGRES_PORT = 5432
POSTGRES_DB = "trainings_ztbd"
POSTGRES_USER = "admin"
POSTGRES_PASSWORD = "password"

# Dane do tabel

exercises = [
    'Bench Press', 'Squat', 'Deadlift', 'Overhead Press', 'Pull Up',
    'Push Up', 'Bicep Curl', 'Tricep Extension', 'Leg Press', 'Lateral Raise',
    'Dumbbell Row', 'Plank', 'Lunge', 'Leg Curl', 'Chest Fly',
    'Cable Crossover', 'Seated Row', 'Lat Pulldown', 'Face Pull', 'Calf Raise'
]
muscles = [
    'Chest', 'Shoulders', 'Biceps', 'Triceps', 'Lats', 'Traps',
    'Abs', 'Side Abs', 'Lower Back', 'Quads', 'Hamstrings',
    'Glutes', 'Calves', 'Lower Calves', 'Upper Back', 'Serratus',
    'Upper Arm', 'Forearm Flexors', 'Forearm Extensors', 'Inner Thighs'
]


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
            first_name TEXT,
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
            muscle_id INT REFERENCES Muscles(id)
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
            plan_id INT REFERENCES Plans(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS UsersPlans (
            user_id INT REFERENCES Users(id),
            trainer_id INT REFERENCES Users(id),
            plan_id INT REFERENCES Plans(id)
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
            training_id INT REFERENCES Trainings(id)
        );
        """
    ]

    for query in create_table_queries:
        cursor.execute(query)
        logger.info("Executed query: %s", query.strip())

    conn.commit()
    cursor.close()


def insert_users_into_postgres(conn, num_rows):
    cursor = conn.cursor()

    for _ in range(num_rows):
        fake = faker.Faker()
        name = fake.first_name()
        surname = fake.last_name()
        email = fake.email()
        password = fake.password()
        is_trainer = random.choices([True, False], weights=[0.4, 0.6])[0]
        gender = random.choice(['male', 'female'])
        weight = random.randint(50, 100)
        age = random.randint(18, 80)

        insert_query = sql.SQL(
            "INSERT INTO users (first_name, surname, email, password, isTrainer, gender, weight, age) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")

        cursor.execute(insert_query, (name, surname, email, password, is_trainer, gender, weight, age))

    conn.commit()
    cursor.close()


def insert_exercises_names_into_postgres(conn):
    cursor = conn.cursor()

    for exercise in exercises:
        insert_query = sql.SQL("INSERT INTO exercisesname (en_name) VALUES (%s)")
        cursor.execute(insert_query, (exercise,))

    conn.commit()
    cursor.close()


def insert_muscles_names_into_postgres(conn):
    cursor = conn.cursor()

    for muscle in muscles:
        insert_query = sql.SQL("INSERT INTO muscles (muscle_name) VALUES (%s)")
        cursor.execute(insert_query, (muscle,))

    conn.commit()
    cursor.close()


def insert_exercises_into_postgres(conn):
    cursor = conn.cursor()

    for i in range(len(exercises)):
        insert_query = sql.SQL(
            "INSERT INTO exercises (exercise_name_id, description, gif, tags) VALUES (%s, %s, %s, %s)")
        cursor.execute(insert_query, (i + 1, exercises[i] + " description", "path to gif", ['a', 'b', 'c'],))
    conn.commit()
    cursor.close()


def insert_exercise_muscles_into_postgres(conn):
    cursor = conn.cursor()
    for i in range(len(exercises)):
        insert_query = sql.SQL("INSERT INTO  exercisesmuscles (exercise_id, muscle_id) VALUES (%s, %s)")
        cursor.execute(insert_query, (i + 1, i + 1,))
    conn.commit()
    cursor.close()


def insert_plans_into_postgres(conn, num_rows):
    cursor = conn.cursor()
    exercise = len(exercises)
    for i in range(num_rows):
        insert_query = sql.SQL("INSERT INTO plans (weightsequences, date) VALUES (%s, %s)")
        insert_query_sec = sql.SQL("INSERT INTO  usersplans (user_id, trainer_id, plan_id) VALUES  (%s, %s, %s)")
        insert_query_third = sql.SQL("INSERT INTO exerciseplans (exercise_id, plan_id) VALUES (%s, %s)")
        cursor.execute(insert_query, (f"sequence{i}", date.today(),))

    conn.commit()
    cursor.close()


def insert_plans_data_into_postgres(conn, num_rows):
    cursor = conn.cursor()
    exercise = len(exercises)
    for i in range(num_rows):
        insert_query_sec = sql.SQL("INSERT INTO  usersplans (user_id, trainer_id, plan_id) VALUES  (%s, %s, %s)")
        insert_query_third = sql.SQL("INSERT INTO exercisesplans (exercise_id, plan_id) VALUES (%s, %s)")

        cursor.execute(insert_query_sec, (random.randint(1, 100), random.randint(1, 100), i + 1), )

        cursor.execute(insert_query_third, (random.randint(1, exercise), i + 1))

    conn.commit()
    cursor.close()


def insert_trainings_into_postgres(conn, num_rows):
    cursor = conn.cursor()
    for i in range(num_rows):
        insert_query = sql.SQL("INSERT INTO trainings (date, user_id, plan_id) VALUES (%s, %s, %s)")
        cursor.execute(insert_query, (date.today(), random.randint(1, 100), random.randint(1, num_rows),))

    conn.commit()
    cursor.close()


def insert_trainings_exercises_into_postgres(conn, num_rows):
    cursor = conn.cursor()
    exercise = len(exercises)
    for i in range(num_rows):
        insert_query = sql.SQL("INSERT INTO trainingexercises (exercise_id, sets, rep, weight) VALUES (%s, %s, %s, %s)")
        cursor.execute(insert_query, (
        random.randint(1, exercise), random.randint(1, 5), random.randint(1, 20), random.randint(1, 200),))

    conn.commit()
    cursor.close()


def insert_train_trainings_exercises_into_postgres(conn, num_rows):
    cursor = conn.cursor()
    for i in range(num_rows):
        insert_query = sql.SQL("INSERT INTO ttexercises(trainingexercise_id, training_id) VALUES (%s, %s)")
        cursor.execute(insert_query, (random.randint(1, num_rows), random.randint(1, num_rows),))

def initial_insert_into_postgres(postgres_conn, rows):
    create_tables_postgres(postgres_conn)
    insert_users_into_postgres(postgres_conn, rows)
    insert_exercises_names_into_postgres(postgres_conn)
    insert_muscles_names_into_postgres(postgres_conn)
    insert_exercises_into_postgres(postgres_conn)
    insert_exercise_muscles_into_postgres(postgres_conn)
    insert_trainings_exercises_into_postgres(postgres_conn, rows)
    insert_plans_into_postgres(postgres_conn, rows)
    insert_plans_data_into_postgres(postgres_conn, rows)
    insert_trainings_into_postgres(postgres_conn, rows)
    insert_train_trainings_exercises_into_postgres(postgres_conn, rows)

def main():
    rows = 1000
    postgres_conn = get_postgres_connection()

    #TODO: Jak będziesz to puszczał to puść tego initial_inserta najpierw dla jakiejś małej liczby danych i dopiero potem mierz
    # czas dla tych dwóch insertów co tam są w tym mierzeniu czasu, bo tu wszystkie tabele są inicjowane bo inaczej błąd wywalało


    initial_insert_into_postgres(postgres_conn, rows)

    start_time_postgres = datetime.now()

    # insert_plans_into_postgres(postgres_conn, rows)
    # insert_plans_data_into_postgres(postgres_conn, rows)


    end_time_postgres = datetime.now()

    duration_postgres = end_time_postgres - start_time_postgres
    logger.info("Czas wstawiania danych do bazy PostgreSQL: %s", duration_postgres)

    postgres_conn.close()


if __name__ == "__main__":
    main()
