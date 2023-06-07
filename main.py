import csv
from pymongo import MongoClient
# from geopy.geocoders import Bing
# import time

csv_file = 'data.csv'
data = []

with open(csv_file, 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    header = next(reader)
    for row in reader:
        # noinspection PyTypeChecker
        data.append(dict(row))

client = MongoClient('mongodb://localhost:27017')

db = client['project1']
collection = db['Restaurants']

collection.insert_many(data)

collection.update_many(
    {"Reviews": {"$regex": "^[0-5]"}},
    [
        {
            "$set": {
                "Reviews": {
                    "$toDouble": "$Reviews"
                }
            }
        }
    ]
)

collection.update_many(
    {"No of Reviews": {"$regex": "^[0-9]"}},
    [
        {
            "$set": {
                "No of Reviews": {
                    "$toDouble": "$No of Reviews"
                }
            }
        }
    ]
)

# geolocator = Bing(api_key='AhpwrCFQAHjj_6XelmahpGUxECXe1tonsrdoV2zAc9VJETqAj-6ekcmoMaKv5Ri6')
#
#
# for doc in collection.find():
#
#     full_address = " ".join([doc['Street Address'], ",", doc['Location']])
#     location = geolocator.geocode(full_address)
#
#     # Check if the location was found
#     if location is not None:
#         collection.update_one({'_id': doc['_id']},
#                               {'$set': {"latitude": location.latitude, "longitude": location.longitude}})
#     else:
#         print(f'Location not found for document {doc["_id"]}')
#
#     time.sleep(1)


