import random
from pymongo import MongoClient


client = MongoClient("mongodb://localhost:27017/")
db = client["project1"]
restaurants_collection = db["Restaurants"]
people_collection = db["people"]
restaurants_collection.create_index([("coordinates", "2dsphere")])

new_customers = [
    {
        "name": "Tom Hanks",
        "restaurant_visited": "Coach House Diner",
        "address": "55 State Rt 4",
        "score": 4.5
    },
    {
        "name": "Will Smith",
        "restaurant_visited": "John Thomas Steakhouse",
        "address": "1152 Danby Rd",
        "score": 3.8
    },
    {
        "name": "Leonardo DiCaprio",
        "restaurant_visited": "Revolve True Food & Wine Bar",
        "address": "10024 Main St",
        "score": 4.2
    },
    {
        "name": "Brad Pitt",
        "restaurant_visited": "Social 37",
        "address": "2 Route 37 W Unit B2",
        "score": 4
    },
    {
        "name": "Scarlett Johansson",
        "restaurant_visited": "Magic Castle",
        "address": "7001 Franklin Ave",
        "score": 5
    },
    {
        "name": "Johnny Depp",
        "restaurant_visited": "The Cheesecake Factory",
        "address": "401 Bellevue Sq",
        "score": 4.5
    },
    {
        "name": "George Clooney",
        "restaurant_visited": "Off Vine",
        "address": "6263 Leland Way",
        "score": 4.6
    },
    {
        "name": "Scarlett Johansson",
        "restaurant_visited": "Elements Restaurant",
        "address": "907 Main Street",
        "score": 4
    },
    {
        "name": "Johnny Depp",
        "restaurant_visited": "Henry Street Taproom",
        "address": "86 Henry St",
        "score": 3.5
    },
    {
        "name": "just a test",
        "restaurant_visited": "test test test",
        "address": "somewhere",
        "score": 5
    }
]

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


while len(new_customers) < 20:  # Specify the number of customers you want to generate
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
        new_customers.append({
            "name": full_name,
            "restaurant_visited": restaurant_name,
            "address": address,
            "score": float(score)
        })




