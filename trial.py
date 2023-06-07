#
# import pika
# from pymongo import MongoClient
#
# # Connect to MongoDB
# client = MongoClient("mongodb://localhost:27017/")
# db = client["project1"]
# restaurants_collection = db["Restaurants"]
# people_collection = db["people"]
#
#
# connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
# channel = connection.channel()
#
#
# channel.exchange_declare(exchange="customers", exchange_type="direct")
#
#
# restaurants_queue = channel.queue_declare(queue="Restaurants", exclusive=False).method.queue
# people_queue = channel.queue_declare(queue="people", exclusive=False).method.queue
# channel.queue_bind(exchange="customers", queue=restaurants_queue, routing_key="")
# channel.queue_bind(exchange="customers", queue=people_queue, routing_key="")
#
#
# def restaurant_callback(ch, method, properties, body):
#     try:
#         # Extract data from the message
#         print(body)
#         message_parts = body.decode().split(" visited ")
#         restaurant_name = message_parts[1].split(" and gave a score of ")[0]
#         person_name = message_parts[0]
#
#         # Append the restaurant to the "people" collection
#         people_collection.insert_one({"name": person_name, "restaurant_visited": restaurant_name})
#
#         # Acknowledge the message
#         ch.basic_ack(delivery_tag=method.delivery_tag)
#     except Exception as e:
#         # Handle any exceptions
#         print(f"An error occurred while processing the people message: {e}")
#
#
# def people_callback(ch, method, properties, body):
#     try:
#         # Extract data from the message
#         message_parts = body.decode().split(" visited ")
#         restaurant_name = message_parts[1].split(" and gave a score of ")[0]
#         score = float(message_parts[1].split(" and gave a score of ")[1])
#
#         query = {"Name": restaurant_name}
#         update = {"$inc": {"No of Reviews": 1}, "$set": {"new_score": score}}
#         restaurants_collection.update_one(query, update)
#
#         ch.basic_ack(delivery_tag=method.delivery_tag)
#     except Exception as e:
#         # Handle any exceptions
#         print(f"An error occurred while processing the people message: {e}")
#
#
# try:
#     channel.basic_consume(queue=restaurants_queue, on_message_callback=restaurant_callback)
#     channel.basic_consume(queue=people_queue, on_message_callback=people_callback)
#     channel.start_consuming()
# except KeyboardInterrupt:
#     channel.stop_consuming()
#
# connection.close()

# from geopy.geocoders import Nominatim
# geolocator = Nominatim(user_agent="hello")
# location = geolocator.geocode("55 State Rt 4, Hackensack, NJ 07601-6337")
# print(location.address)
# print((location.latitude, location.longitude))
#
from geopy.geocoders import Bing

#Create a geocoder object using Bing
# geolocator = Bing(api_key='AhpwrCFQAHjj_6XelmahpGUxECXe1tonsrdoV2zAc9VJETqAj-6ekcmoMaKv5Ri6')
#
# street = "0"
# city = "Poughkeepsie, NY"
# adddd = " ".join([street, ",", city])
# # Geocode an address using Bing
# location = geolocator.geocode(adddd)

# # Check if the location was found
# if location is not None:
#     print("Latitude:", location.latitude)
#     print("Longitude:", location.longitude)
#     print("Address:", location.address)
# else:
#     print("Location not found.")

from fastapi import FastAPI, Path, Query, HTTPException, status
from pydantic import BaseModel, Field
from typing import Optional
from pymongo import MongoClient
#
# # Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["project1"]
restaurants_collection = db["Restaurants"]
people_collection = db["people"]
# restaurants_list = restaurants_collection.find({"Location": {'$regex': 'NY', "$options": "i"}})
# restaurants = restaurants_list
# for restaurant in restaurants:
#     restaurant["_id"] = str(restaurant["_id"])  # Convert ObjectId to string
#     return restaurants


# restaurants_list = restaurants_collection.find({"Location": {'$regex': 'NY', "$options": "i"}})
# data = []
# for restaurant in restaurants_list:
#     restaurant["_id"] = str(restaurant["_id"])
#     data.append(restaurant)
# print(data)


# data = []
# print(f"State received: NY")  # print the received state
# query = {"Location": {'$regex': "NY", "$options": "i"}}
# print(f"Query to execute: {query}")  # print the query to be executed
# restaurants_list = restaurants_collection.find(query)
#
#     # Fetch the restaurants into a list with a limit
# restaurants = restaurants_list
#
# print(f"Restaurants fetched: {restaurants}")  # print the fetched restaurants
#
# if not restaurants:
#     raise HTTPException(status_code = 404, detail = "Restaurant not found")
# else:
#     for restaurant in restaurants:
#         if 'Location' in restaurant:  # check if 'Location' field exists
#             restaurant["_id"] = str(restaurant["_id"])
#             print(f"Restaurant processed: {restaurant}")  # print each restaurant
#             data.append(restaurant)
#     print(data)

# types_list = []
# query = restaurants_collection.find({"Name": {'$regex': "the cheesecake factory", '$options': 'i'},
#                                      "Location": {'$regex': 'Bellevue, WA', '$options': 'i'}})
# for rest in query:
#     types = rest['Type'].split(', ')
#     types_list.extend(types)
#     print(types_list)

# import sys
# print(sys.prefix)
visited_restaurants = []
persons = people_collection.find({"name": {'$regex': 'Tom Hanks', '$options': 'i'}})
for person in persons:
    for visit in person.get('restaurant_visited', []):
        visited_restaurants.append(visit['restaurant'])
print(visited_restaurants)

types_list = []
for restaurant in visited_restaurants:
    query = restaurants_collection.find({"Name": {'$regex': restaurant, '$options': 'i'}})
    for rest in query:
        types = rest['Type'].split(', ')
        types_list.extend(types)
print(list(set(types_list)))

new_query = restaurants_collection.find({'Location': {'$regex': location, '$options': 'i'},
                                                 'Type': {'$regex': rest_type, '$options': 'i'},
                                                 'Reviews': {'$gte': 4}})
print(f"{obj['Street Address']}, {obj['Location']}")








# def find_matching_restaurants(location, rest_type):
#     return restaurants_collection.find({'Location': {'$regex': location, '$options': 'i'},
#                                         'Type': {'$regex': rest_type, '$options': 'i'},
#                                         'Reviews': {'$gte': 4}})
#
#
# def find_coordinates(restaurant):
#     coordinates = get_geo_coordinates(f"{restaurant['Street Address']}, {restaurant['Location']}")
#     if coordinates is not None:
#         restaurants_collection.update_one({'_id': restaurant['_id']}, {'$set': {'coordinates': coordinates}})
#     return restaurant
#
#
# def get_nearby_restaurants(location, types, point_location):
#     data = []
#     seen_ids = set()
#     for rest_type in types:
#         matching_restaurants = find_matching_restaurants(location, rest_type)
#         updated_restaurant = [find_coordinates(restaurant) for restaurant in matching_restaurants]
#
#         for restaurant in updated_restaurant:
#             nearby_restaurants = restaurants_collection.find(
#                 {'coordinates':
#                      {'$near':
#                           {'$geometry': point_location,
#                            '$maxDistance': 3000  # max distance in meters
#                            }
#                       }
#                  }
#             )
#             for nearby_restaurant in nearby_restaurants:
#                 if str(nearby_restaurant["_id"]) not in seen_ids:
#                     data.append(nearby_restaurant)
#                     seen_ids.add(str(nearby_restaurant["_id"]))
#
#     return data
