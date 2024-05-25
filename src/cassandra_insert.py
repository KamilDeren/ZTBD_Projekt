import uuid
from datetime import datetime
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
            exercises LIST<TEXT>,
            weight_sequence LIST<FLOAT>,
            date TIMESTAMP,
            PRIMARY KEY (user_id, date, plan_id)
        ) WITH CLUSTERING ORDER BY (date DESC, plan_id ASC);
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


def initial_insert_cassandra(session):
    for _ in range(100):
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


def insert_data_into_cassandra(session, num_rows):
    for _ in range(num_rows):
        user_id = uuid.uuid4()
        plan_id = uuid.uuid4()
        trainer_id = uuid.uuid4()
        exercises = [fake.word() for _ in range(fake.random_int(min=1, max=10))]
        weight_sequence = [round(fake.random_number(digits=2, fix_len=False) + fake.random.random(), 1)
                           for _ in range(len(exercises))]
        date = fake.date_time_this_year()

        plan_query = session.prepare(
            "INSERT INTO plans (user_id, plan_id, trainer_id, exercises, weight_sequence, date) VALUES (:user_id, "
            ":plan_id, :trainer_id, :exercises, :weight_sequence, :date)"
        )

        session.execute(plan_query.bind({
            'user_id': user_id,
            'plan_id': plan_id,
            'trainer_id': trainer_id,
            'exercises': exercises,
            'weight_sequence': weight_sequence,
            'date': date
        }))


def main():
    cassandra_session = get_cassandra_session()

    create_tables_cassandra(cassandra_session)
    initial_insert_cassandra(cassandra_session)

    start_time_cassandra = datetime.now()
    insert_data_into_cassandra(cassandra_session, 200)
    end_time_cassandra = datetime.now()

    duration_cassandra = end_time_cassandra - start_time_cassandra
    logger.info("Czas wstawiania danych do bazy Cassandra: %s", duration_cassandra)

    cassandra_session.shutdown()


if __name__ == "__main__":
    main()
