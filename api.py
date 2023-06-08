from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from typing import Optional
from pymongo import MongoClient
import os
from geopy import Bing
from dotenv import load_dotenv

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["project1"]
restaurants_collection = db["Restaurants"]
people_collection = db["people"]
restaurants_collection.create_index([("coordinates", "2dsphere")])

app = FastAPI()


@app.get("/restaurants")
async def show_restaurants():
    data = []
    restaurants = restaurants_collection.find({})
    for rest in restaurants:
        rest["_id"] = str(rest["_id"])
        data.append(rest)
    return {"data": data}


@app.get("/people")
async def show_people():
    data = []
    people = people_collection.find({})
    for person in people:
        person["_id"] = str(person["_id"])
        data.append(person)
    return {"data": data}


@app.get("/restaurants/name/{restaurant_name}", status_code = status.HTTP_200_OK)
async def restaurant_by_name(restaurant_name: str):
    data = []
    restaurants_list = restaurants_collection.find({"Name": {'$regex': restaurant_name, '$options': 'i'}})
    for restaurant in restaurants_list:

        restaurant["_id"] = str(restaurant["_id"])
        data.append(restaurant)
    return data


@app.get("/restaurant/state/{state}")
async def show_restaurants_by_state(state):
    data = []
    new_state = str(state)
    restaurants = restaurants_collection.find({"Location": {'$regex': new_state, "$options": "i"}})
    for rest in restaurants:
        rest["_id"] = str(rest["_id"])
        data.append(rest)
    return data


@app.get("/restaurant/score/{min_score}")
async def show_restaurants_by_score(min_score: float):
    data = []
    restaurants = restaurants_collection.find({"Reviews": {'$gte': min_score}})
    for rest in restaurants:
        rest["_id"] = str(rest["_id"])
        data.append(rest)
    return data


# a format for the restaurant creation:
class NewRestaurant(BaseModel):
    Name: str = Field(min_length = 1)
    Street_Address: str = Field(min_length = 1)
    Location: str
    Type: str
    Reviews: Optional[float] = 0
    No_of_Reviews: Optional[float] = 0
    Comments: Optional[str]
    Contact_Number: Optional[str]


@app.post("/restaurant/create_restaurant")
async def create_restaurant(new_restaurant: NewRestaurant):
    restaurant_to_add = NewRestaurant(Name = new_restaurant.Name, Street_Address = new_restaurant.Street_Address,
                                      Location = new_restaurant.Location, Type = new_restaurant.Type,
                                      Reviews = new_restaurant.Reviews, No_of_Reviews = new_restaurant.No_of_Reviews,
                                      Comments = new_restaurant.Comments,
                                      Contact_Number = new_restaurant.Contact_Number)

    restaurants_collection.insert_one(restaurant_to_add.dict())


# class NewCustomer(BaseModel):
#     name: str
#     restaurant_visited: str
#     address: str
#     score: float


# @app.post("/customers/create_customer")
# async def create_customers(new_customer: NewCustomer):
#     customer_to_add = NewCustomer(name = new_customer.name,
#                                   restaurant_visited = new_customer.restaurant_visited,
#                                   address = new_customer.address,
#                                   score = new_customer.score)
#
#     new_customers.put(customer_to_add)


class UpdateRestaurantDetails(BaseModel):
    Name: Optional[str] = Field(None)
    Street_Address: Optional[str] = Field(None)
    Location: Optional[str] = Field(None)
    Type: Optional[str] = Field(None)
    Comments: Optional[str] = Field(None)
    Contact_Number: Optional[str] = Field(None)


@app.put("/restaurant/update/{restaurant_name}/{address}")
async def update_restaurant_details(restaurant_name: str, address: str, restaurant_details: UpdateRestaurantDetails):
    query = {"Name": restaurant_name, 'Street Address': address}
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

    if response.matched_count:
        return {"message": f"Restaurant '{restaurant_name}' details updated successfully."}
    else:
        raise HTTPException(status_code = 404, detail = f"Restaurant '{restaurant_name}' not found.")


@app.delete("/restaurant/delete/{restaurant_name}/{address}", status_code = status.HTTP_204_NO_CONTENT)
async def delete_restaurant(restaurant_name: str, address: str):
    response = restaurants_collection.delete_one({'Name': restaurant_name, 'Street Address': address})
    if response.deleted_count:
        return {"message": f"Restaurant {restaurant_name} at {address} deleted."}
    else:
        raise HTTPException(status_code = 404, detail = f"Restaurant {restaurant_name} at {address} not found")


load_dotenv()  # take environment variables from .env.
API_KEY = os.getenv('BING_API_KEY')
geolocator = Bing(api_key = 'AhpwrCFQAHjj_6XelmahpGUxECXe1tonsrdoV2zAc9VJETqAj-6ekcmoMaKv5Ri6', timeout = 10)


@app.get("/restaurant/recommendation/{full_name}/{state}/{city}/{street}")
async def recommend_restaurant(full_name: str, state: str, city: str, street=Optional[str]):
    location = get_location(street, city, state)
    my_point_location = get_geo_coordinates(location)
    persons_visited_restaurants = get_visited_restaurants(full_name)

    if not persons_visited_restaurants:
        raise HTTPException(status_code = 404, detail = 'There is no such person. You should create customer first')

    unique_types = get_unique_types(persons_visited_restaurants)
    nearby_restaurants = get_nearby_restaurants(f"{city}, {state}", unique_types, my_point_location)

    for restaurant in nearby_restaurants:
        restaurant['_id'] = str(restaurant['_id'])

    return nearby_restaurants


def get_location(street, city, state):
    if street is None:
        return f"{city}, {state}"
    else:
        return f"{street}, {city}, {state}"


def get_geo_coordinates(location):
    coordinates = geolocator.geocode(location)
    return {'type': 'Point', 'coordinates': [coordinates.longitude, coordinates.latitude]}


def get_visited_restaurants(full_name):
    visited_restaurants = []
    persons = people_collection.find({"name": {'$regex': full_name, '$options': 'i'}})
    for person in persons:
        for visit in person.get('restaurant_visited', []):
            visited_restaurants.append(visit['restaurant'])
    return visited_restaurants


def get_unique_types(restaurants):
    types_list = []
    for restaurant in restaurants:
        query = restaurants_collection.find({"Name": {'$regex': restaurant, '$options': 'i'}})
        for rest in query:
            types = rest['Type'].split(', ')
            types_list.extend(types)
    return list(set(types_list))


def get_nearby_restaurants(location, types, point_location):
    data = []
    seen_ids = set()
    for rest_type in types:
        new_query = restaurants_collection.find({'Location': {'$regex': location, '$options': 'i'},
                                                 'Type': {'$regex': rest_type, '$options': 'i'},
                                                 'Reviews': {'$gte': 4}})
        for obj in new_query:
            coordinates = get_geo_coordinates(f"{obj['Street Address']}, {obj['Location']}")
            if coordinates is not None:
                restaurants_collection.update_one({'_id': obj['_id']}, {'$set': {'coordinates': coordinates}})

        nearby_restaurants = restaurants_collection.find(
            {'coordinates':
                 {'$near':
                      {'$geometry': point_location,
                       '$maxDistance': 5000  # max distance in meters
                       }
                  }
             }
        )
        for restaurant in nearby_restaurants:
            if str(restaurant["_id"]) not in seen_ids:
                data.append(restaurant)
                seen_ids.add(str(restaurant["_id"]))
    return data


