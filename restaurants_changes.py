from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from typing import Optional
from pymongo import MongoClient

client = MongoClient("mongodb://host.docker.internal:27017/")
db = client["project1"]
restaurants_collection = db["Restaurants"]
people_collection = db["people"]
restaurants_collection.create_index([("coordinates", "2dsphere")])


def call_people():
    people = people_collection.find({})
    data = [{**person, '_id': str(person['_id'])} for person in people]
    return {"data": data}


def restaurants_query(query):
    restaurants = restaurants_collection.find(query)
    data = [{**rest, '_id': str(rest['_id'])} for rest in restaurants]
    return data


def insert_restaurant(restaurant_to_add):
    restaurants_collection.insert_one(restaurant_to_add.dict())


def restaurants_to_update(restaurant_name, address, restaurant_details):
    query = {"Name": {'$regex': restaurant_name, '$options': 'i'}, 'Street Address': address}
    new_values = {"$set": {}}
    if restaurant_details.Name is not None:
        new_values["$set"]["Name"] = restaurant_details.Name
    if restaurant_details.Street_Address is not None:
        new_values["$set"]["Street Address"] = restaurant_details.Street_Address
    if restaurant_details.Location is not None:
        new_values["$set"]["Location"] = restaurant_details.Location
    if restaurant_details.Type is not None:
        new_values["$set"]["Type"] = restaurant_details.Type
    if restaurant_details.Comments is not None:
        new_values["$set"]["Comments"] = restaurant_details.Comments
    if restaurant_details.Contact_Number is not None:
        new_values["$set"]["Contact Number"] = restaurant_details.Contact_Number

    response = restaurants_collection.update_one(query, new_values)
    return response


def restaurants_to_delete(restaurant_name, address):
    if address:
        response = restaurants_collection.delete_one({'Name': restaurant_name, 'Street Address': address})
    else:
        response = restaurants_collection.delete_many({'Name': restaurant_name})
    return response
