from pymongo import MongoClient
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Konfiguracja bazy danych MongoDB
MONGO_HOST = "127.0.0.1"
MONGO_PORT = 27017
MONGO_DB = "trainings_ztbd"
MONGO_COLLECTION = "users"
MONGO_USER = "admin"
MONGO_PASSWORD = "password"

def get_mongo_client():
    uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource=admin"
    client = MongoClient(uri)
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
    mongo_client = get_mongo_client()

    start_time_mongodb = datetime.now()
    insert_data_into_mongodb(mongo_client, 20)
    end_time_mongodb = datetime.now()

    duration_mongodb = end_time_mongodb - start_time_mongodb
    logger.info("Czas wstawiania danych do bazy MongoDB: %s", duration_mongodb)

    mongo_client.close()

if __name__ == "__main__":
    main()
