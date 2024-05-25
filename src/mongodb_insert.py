from bson import ObjectId
from pymongo import MongoClient
from datetime import datetime
import logging
from faker import Faker
from faker import providers
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

# Konfiguracja bazy danych MongoDB
MONGO_HOST = "127.0.0.1"
MONGO_PORT = 27017
MONGO_DB = "trainings_ztbd"
MONGO_USER = "admin"
MONGO_PASSWORD = "password"


def get_mongo_client():
    uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource=admin"
    client = MongoClient(uri)
    return client


def initial_insert_mongodb(client, rows):
    db = client[MONGO_DB]
    users = db['users']

    trainer_ids = []
    user_ids = []

    for _ in range(rows):
        is_trainer = random.randint(0, 99) < 20
        user_id = ObjectId()
        if is_trainer:
            trainer_ids.append(user_id)
        else:
            user_ids.append(user_id)

        user_data = {
            "_id": user_id,
            "firstName": fake.first_name(),
            "lastName": fake.last_name(),
            "email": fake.email(),
            "password": fake.password(),
            "isTrainer": is_trainer,
            "gender": fake.random_element(elements=('Male', 'Female')),
            "weight": round(random.uniform(50, 100), 1),
            "age": fake.random_int(min=18, max=70),
        }
        users.insert_one(user_data)

    return trainer_ids, user_ids


def insert_data_into_mongodb(client, trainer_ids, user_ids, rows):
    db = client[MONGO_DB]
    trainings = db['trainings']

    trainer_id = random.choice(trainer_ids)
    user_id = random.choice(user_ids)

    # Przydzielanie planów treningowych trenującym
    for _ in range(rows):
        exercises = []
        for _ in range(5):  # każdy plan ma 5 ćwiczeń/przerw
            exercise = {
                "name": fake.word(),
                "sets": fake.random_int(min=1, max=5),
                "reps": fake.random_int(min=5, max=15),
                "weight": fake.random_int(min=10, max=100),
                "restTime": fake.random_int(min=30, max=90),
                "type": "exercise"
            }
            rest = {
                "name": "Przerwa",
                "time": fake.random_int(min=60, max=180),
                "type": "pause"
            }
            exercises.extend([exercise, rest])
        training_data = {
            "userId": user_id,
            "trainerId": trainer_id,
            "exercises": exercises
        }
        trainings.insert_one(training_data)


def delete_data_mongodb(client):
    db = client[MONGO_DB]
    trainings_collection = db['trainings']

    trainings_collection.delete_many({})


def clear_mongodb(client):
    db = client[MONGO_DB]
    users = db['users']

    users.delete_many({})


def main():
    mongo_client = get_mongo_client()

    trainer_ids, user_ids = initial_insert_mongodb(mongo_client, 200)

    start_time_mongodb = datetime.now()
    insert_data_into_mongodb(mongo_client, trainer_ids, user_ids, 10000)
    end_time_mongodb = datetime.now()

    duration_mongodb = end_time_mongodb - start_time_mongodb
    logger.info("Czas wstawiania danych do bazy MongoDB: %s", duration_mongodb)

    start_time_mongodb = datetime.now()
    delete_data_mongodb(mongo_client)
    end_time_mongodb = datetime.now()

    duration_mongodb = end_time_mongodb - start_time_mongodb
    logger.info("Czas usuwania danych do bazy MongoDB: %s", duration_mongodb)

    clear_mongodb(mongo_client)
    mongo_client.close()


if __name__ == "__main__":
    main()
