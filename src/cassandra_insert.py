import uuid
from datetime import datetime
import random
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from faker import Faker
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
fake = Faker()

# Konfiguracja bazy danych Cassandra
KEYSPACE_NAME = "trainings_ztbd"
CASSANDRA_CONTACT_POINTS = ["127.0.0.1"]
CASSANDRA_PORT = 9042
USERNAME = "admin"
PASSWORD = "password"


def get_cassandra_session():
    auth_provider = PlainTextAuthProvider(username=USERNAME, password=PASSWORD)
    cluster = Cluster(contact_points=CASSANDRA_CONTACT_POINTS, port=CASSANDRA_PORT, auth_provider=auth_provider, protocol_version = 5)
    session = cluster.connect()
    create_keyspace_query = f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE_NAME}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }}
        """
    session.execute(create_keyspace_query)
    session.set_keyspace(KEYSPACE_NAME)
    return session


def create_tables(session):
    create_table_queries = [
        """
        CREATE TABLE IF NOT EXISTS trainings (
            training_id UUID PRIMARY KEY,
            title TEXT,
            type TEXT,
            description TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS plans (
            user_id UUID,
            plan_id UUID,
            trainer_id UUID,
            exercise TEXT,
            weight_sequence LIST<FLOAT>,
            date TIMESTAMP,
            PRIMARY KEY (exercise)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id UUID PRIMARY KEY,
            name TEXT,
            surname TEXT,
            email TEXT,
            pass TEXT,
            isTrainer BOOLEAN,
            gender TEXT,
            weight DOUBLE,
            age INT
        );
        """
    ]

    for query in create_table_queries:
        session.execute(query)


def initial_insert(session):
    for _ in range(200):
        training_id = uuid.uuid4()
        title = fake.catch_phrase()
        training_type = fake.word(ext_word_list=['Cardio', 'Strength', 'Flexibility', 'Balance'])
        description = fake.text()

        training_query = session.prepare(
            "INSERT INTO trainings (training_id, title, type, description) VALUES (:training_id, :title, :type, "
            ":description)"
        )

        session.execute(training_query.bind({
            'training_id': training_id,
            'title': title,
            'type': training_type,
            'description': description
        }))

        user_id = uuid.uuid4()
        name = fake.first_name()
        surname = fake.last_name()
        email = fake.email()
        password = fake.password()
        is_trainer = fake.boolean()
        gender = fake.random_element(elements=('Male', 'Female'))
        weight = round(fake.random_number(digits=2, fix_len=False) + fake.random.random(), 1)
        age = fake.random_int(min=18, max=80)

        user_query = session.prepare(
            "INSERT INTO users (user_id, name, surname, email, pass, isTrainer, gender, weight, age) VALUES ("
            ":user_id, :name, :surname, :email, :pass, :isTrainer, :gender, :weight, :age)"
        )

        session.execute(user_query.bind({
            'user_id': user_id,
            'name': name,
            'surname': surname,
            'email': email,
            'pass': password,
            'isTrainer': is_trainer,
            'gender': gender,
            'weight': weight,
            'age': age
        }))


def prepare_data(num_rows):
    exercises = [
        'Bench Press', 'Squat', 'Deadlift', 'Overhead Press', 'Pull Up',
        'Push Up', 'Bicep Curl', 'Tricep Extension', 'Leg Press', 'Lateral Raise',
        'Dumbbell Row', 'Plank', 'Lunge', 'Leg Curl', 'Chest Fly',
        'Cable Crossover', 'Seated Row', 'Lat Pulldown', 'Face Pull', 'Calf Raise'
    ]
    data = []

    for _ in range(num_rows):
        user_id = uuid.uuid4()
        plan_id = uuid.uuid4()
        trainer_id = uuid.uuid4()
        exercise = random.sample(exercises, 1)[0]
        weight_sequence = [round(fake.random_number(digits=2, fix_len=False) + fake.random.random(), 1)
                           for _ in range(len(exercise))]
        date = fake.date_time_this_year()

        data.append({
            'user_id': user_id,
            'plan_id': plan_id,
            'trainer_id': trainer_id,
            'exercise': exercise,
            'weight_sequence': weight_sequence,
            'date': date
        })

    return data


def insert_data(session, data):
    for entry in data:
        plan_query = session.prepare(
            "INSERT INTO plans (user_id, plan_id, trainer_id, exercise, weight_sequence, date) VALUES (:user_id, "
            ":plan_id, :trainer_id, :exercise, :weight_sequence, :date)"
        )

        session.execute(plan_query.bind({
            'user_id': entry['user_id'],
            'plan_id': entry['plan_id'],
            'trainer_id': entry['trainer_id'],
            'exercise': entry['exercise'],
            'weight_sequence': entry['weight_sequence'],
            'date': entry['date']
        }))


def select_data(session):
    session.execute("SELECT * FROM plans WHERE exercise = 'Deadlift'")


def put_data(session):
    session.execute("UPDATE plans SET weight_sequence = [1.0, 2.0, 3.0] WHERE exercise = 'Deadlift'")


def clear(session):
    session.execute("TRUNCATE TABLE trainings")
    session.execute("TRUNCATE TABLE users")


def delete_data(session):
    session.execute("TRUNCATE TABLE plans")


def main():
    for row in [1000, 2000, 5000, 10000, 20000, 50000, 100000]:
        session = get_cassandra_session()

        # Initial inserts
        create_tables(session)
        initial_insert(session)

        # Creating data for insert - here you enter row number
        data_for_insert = prepare_data(row)

        # Insert time measurement
        start = datetime.now()
        insert_data(session, data_for_insert)
        end = datetime.now()

        duration = end - start
        logger.info("Czas wstawiania %s danych do bazy Cassandra: %s", row,  duration)

        # Select time measurement
        start = datetime.now()
        select_data(session)
        end = datetime.now()

        duration = end - start
        logger.info("Czas szukania danych w bazie Cassandra: %s", duration)

        # Put time measurement
        start = datetime.now()
        put_data(session)
        end = datetime.now()

        duration = end - start
        logger.info("Czas zamiany danych w bazie Cassandra: %s", duration)

        # Delete time measurement
        start = datetime.now()
        delete_data(session)
        end = datetime.now()

        duration = end - start
        logger.info("Czas usuwania danych z bazy Cassandra: %s", duration)

        clear(session)

        session.shutdown()


if __name__ == "__main__":
    main()
