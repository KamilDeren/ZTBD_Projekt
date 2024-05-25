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


def initial_insert(client, rows):
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


def insert_data(client, trainer_ids, user_ids, rows):
    db = client[MONGO_DB]
    trainings = db['trainings']
    exercises = [
        'Bench Press', 'Squat', 'Deadlift', 'Overhead Press', 'Pull Up',
        'Push Up', 'Bicep Curl', 'Tricep Extension', 'Leg Press', 'Lateral Raise',
        'Dumbbell Row', 'Plank', 'Lunge', 'Leg Curl', 'Chest Fly',
        'Cable Crossover', 'Seated Row', 'Lat Pulldown', 'Face Pull', 'Calf Raise'
    ]

    trainer_id = random.choice(trainer_ids)
    user_id = random.choice(user_ids)

    # Przydzielanie planów treningowych trenującym
    for _ in range(rows):
        for _ in range(5):  # każdy plan ma 5 ćwiczeń/przerw
            exercise = {
                "name": random.sample(exercises, 2),
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


def select_data(client):
    db = client[MONGO_DB]
    trainings = db['trainings']
    trainings.find({ "exercises": { "$elemMatch": { "name": { "$in": ["Deadlift"] } } } })


def put_data(client):
    db = client[MONGO_DB]
    trainings = db['trainings']

    trainings.update_many(
        {"exercises.name": "Deadlift"},  # Warunek wyszukiwania
        {"$set": {"exercises.$.name": "Bench Press"}}  # Aktualizacja pola name na "Bench Press"
    )


def delete_data(client):
    db = client[MONGO_DB]
    trainings_collection = db['trainings']

    trainings_collection.delete_many({})


def clear(client):
    db = client[MONGO_DB]
    users = db['users']

    users.delete_many({})


def main():
    mongo_client = get_mongo_client()

    trainer_ids, user_ids = initial_insert(mongo_client, 200)

    # Insert time measurement
    start = datetime.now()
    insert_data(mongo_client, trainer_ids, user_ids, 100)
    end = datetime.now()

    duration = end - start
    logger.info("Czas wstawiania danych do bazy MongoDB: %s", duration)

    # Select time measurement
    start = datetime.now()
    select_data(mongo_client)
    end = datetime.now()

    duration = end - start
    logger.info("Czas szukania danych w bazie MongoDB: %s", duration)

    # Put time measurement
    start = datetime.now()
    put_data(mongo_client)
    end = datetime.now()

    duration = end - start
    logger.info("Czas zamiany danych w bazie MongoDB: %s", duration)

    # Delete time measurement
    start = datetime.now()
    delete_data(mongo_client)
    end = datetime.now()

    duration = end - start
    logger.info("Czas usuwania danych z bazy MongoDB: %s", duration)

    clear(mongo_client)
    mongo_client.close()


if __name__ == "__main__":
    main()
