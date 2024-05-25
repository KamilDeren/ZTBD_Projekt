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

def insert_data_into_mongodb(client, num_users, num_trainings):
    db = client[MONGO_DB]
    users = db['users']
    trainings = db['trainings']

    trainer_ids = []
    # Dodajemy trenerów i trenujących
    for _ in range(num_users):
        is_trainer = random.randint(0, 99) < 20  # 20% szans, że osoba jest trenerem
        user_data = {
            "firstName": fake.first_name(),
            "lastName": fake.last_name(),
            "email": fake.email(),
            "password": fake.password(),
            "isTrainer": is_trainer,
            "gender": fake.random_element(elements=('Male', 'Female')),
            "weight": round(random.uniform(50, 100), 1),
            "age": fake.random_int(min=18, max=70),
        }
        result = users.insert_one(user_data)
        if is_trainer:
            trainer_ids.append(result.inserted_id)

    # Każdy trenujący otrzymuje losowego trenera
    trainee_ids = [user['_id'] for user in users.find({"isTrainer": False})]
    for trainee_id in trainee_ids:
        trainer_id = random.choice(trainer_ids)
        # Przydzielanie planów treningowych trenującym
        for _ in range(num_trainings):
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
                "userId": trainee_id,
                "trainerId": trainer_id,
                "exercises": exercises
            }
            trainings.insert_one(training_data)

def main():
    mongo_client = get_mongo_client()

    start_time_mongodb = datetime.now()
    insert_data_into_mongodb(mongo_client, 200, 5)  # 200 użytkowników, każdy z 5 planami treningowymi
    end_time_mongodb = datetime.now()

    duration_mongodb = end_time_mongodb - start_time_mongodb
    logger.info("Czas wstawiania danych do bazy MongoDB: %s", duration_mongodb)

    mongo_client.close()

if __name__ == "__main__":
    main()
