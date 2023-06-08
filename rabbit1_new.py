import queue
import threading
import random
from pymongo import MongoClient
import pika
# from customers import new_customers
import time

client = MongoClient("mongodb://localhost:27017/")
db = client["project1"]
restaurants_collection = db["Restaurants"]
people_collection = db["people"]
restaurants_collection.create_index([("coordinates", "2dsphere")])

first_names = [
    "Olivia", "Ethan", "Ava", "Benjamin", "Amelia", "Liam", "Charlotte",
    "Noah", "Isabella", "Lucas", "Mia", "Alexander", "Sophia", "William",
    "Harper", "James", "Evelyn", "Michael", "Abigail", "Henry", "Emily",
    "Daniel", "Elizabeth", "Jack", "Scarlett", "Samuel", "Grace", "Oliver",
    "Lily", "Joseph"
]

last_names = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis",
    "Garcia", "Rodriguez", "Wilson", "Martinez", "Anderson", "Taylor",
    "Thomas", "Hernandez", "Moore", "Martin", "Jackson", "Thompson", "White",
    "Harris", "Clark", "Lewis", "Young", "Lee", "Walker", "Hall", "Allen",
    "King", "Scott"
]

new_customers = queue.Queue()


def generate_customers():
    while True:
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        full_name = f"{first_name} {last_name}"
        score = random.randint(2, 5)
        restaurant = restaurants_collection.aggregate([
            {"$sample": {"size": 1}},
            {"$project": {"_id": 0, "Name": 1, "Street Address": 1}}
        ])
        for document in restaurant:
            restaurant_name = document["Name"]
            address = document["Street Address"]
            new_customers.put({
                "name": full_name,
                "restaurant_visited": restaurant_name,
                "address": address,
                "score": float(score)
            })


def publish_messages():
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()
    channel.exchange_declare(exchange = "my_exchange", exchange_type = "direct")

    while True:
        customer = new_customers.get()
        time.sleep(2)
        message = f"{customer['name']} visited {customer['restaurant_visited']} at {customer['address']} and gave a score of {customer['score']}"
        print(message)
        channel.basic_publish(exchange = "my_exchange", routing_key = "", body = message)

    connection.close()


threading.Thread(target = generate_customers).start()
threading.Thread(target = publish_messages).start()
