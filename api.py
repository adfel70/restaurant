from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from typing import Optional
from pymongo import MongoClient
import os
from geopy import Bing
from dotenv import load_dotenv
import pandas as pd
import numpy as np

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["project1"]
restaurants_collection = db["Restaurants"]
people_collection = db["people"]
restaurants_collection.create_index([("coordinates", "2dsphere")])

app = FastAPI()


@app.get("/people")
async def show_people():
    data = []
    people = people_collection.find({})
    for person in people:
        person["_id"] = str(person["_id"])
        data.append(person)
    return {"data": data}


@app.get("/restaurants")
async def show_restaurants(name: Optional[str] = None, state: Optional[str] = None, min_score: Optional[float] = None):
    data = []
    query = {}
    if name:
        query["Name"] = {'$regex': name, "$options": "i"}
    if state:
        query["Location"] = {'$regex': state, "$options": "i"}
    if min_score is not None:
        query["Reviews"] = {'$gte': min_score}
    restaurants = restaurants_collection.find(query)
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


class UpdateRestaurantDetails(BaseModel):
    Name: Optional[str] = Field(None)
    Street_Address: Optional[str] = Field(None)
    Location: Optional[str] = Field(None)
    Type: Optional[str] = Field(None)
    Comments: Optional[str] = Field(None)
    Contact_Number: Optional[str] = Field(None)


@app.put("/restaurant/update_restaurant")
async def update_restaurant_details(restaurant_name: str, address: str, restaurant_details: UpdateRestaurantDetails):
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

    if response.matched_count:
        return {"message": f"Restaurant '{restaurant_name}' details updated successfully."}
    else:
        raise HTTPException(status_code = 404, detail = f"Restaurant '{restaurant_name}' not found.")


@app.delete("/restaurant/delete_restaurant", status_code = status.HTTP_204_NO_CONTENT)
async def delete_restaurant(restaurant_name: str, address: Optional[str] = None):
    if address:
        response = restaurants_collection.delete_one({'Name': restaurant_name, 'Street Address': address})
    else:
        response = restaurants_collection.delete_many({'Name': restaurant_name})
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

    if persons_visited_restaurants.empty:
        raise HTTPException(status_code = 404, detail = 'There is no such person. You should create customer first')

    types_scores_df = get_visited_types_df(persons_visited_restaurants)
    weighted_df = weighted_scores_df(types_scores_df)
    set_scored_restaurants(f"{city}, {state}", weighted_df)
    nearby_restaurants = get_nearby_restaurants(f"{city}, {state}", persons_visited_restaurants, my_point_location)

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
    score = []
    persons = people_collection.find({"name": {'$regex': full_name, '$options': 'i'}})
    for person in persons:
        for visit in person.get('restaurant_visited', []):
            visited_restaurants.append(visit['restaurant'])
            score.append(visit['score'])
    new_df = pd.DataFrame({'restaurant': visited_restaurants, 'score': score})
    return new_df


def get_visited_types_df(restaurants_df):
    type_data = []
    for index, row in restaurants_df.iterrows():
        restaurant = row['restaurant']
        score = row['score']
        query = restaurants_collection.find({"Name": {'$regex': restaurant, '$options': 'i'}})
        for rest in query:
            types = rest['Type'].split(', ')
            for t in types:
                new_data = {'type': t, 'score': score}
                type_data.append(new_data)
    type_df = pd.DataFrame(type_data)
    return type_df


def weighted_scores_df(type_df):
    grouped = type_df.groupby('type').agg({'score': ['sum', 'count']}).reset_index()
    grouped.columns = ['type', 'sum_score', 'num_times']
    grouped['new_score'] = grouped['sum_score'] / grouped['num_times']
    grouped['num_times'] = grouped['num_times'].astype(int)
    average_score = type_df['score'].mean()
    new_row = pd.DataFrame({'type': ['default'], 'new_score': [average_score], 'num_times': [1]})
    grouped = pd.concat([grouped, new_row], ignore_index=True)
    grouped['weighted_score'] = grouped['new_score'] * grouped['num_times']
    grouped['log_weighted_score'] = np.log(grouped['weighted_score'])
    return grouped


def set_scored_restaurants(location, dataframe):
    query = restaurants_collection.find({'Location': {'$regex': location, '$options': 'i'},
                                         'Reviews': {'$gte': 4}})
    for rest in query:
        restaurant_score = 0
        types = rest['Type'].split(', ')
        for t in types:
            if dataframe['type'].isin([t]).any():
                restaurant_score += dataframe.loc[dataframe['type'] == t, 'log_weighted_score'].values[0]
            else:
                restaurant_score += dataframe.loc[dataframe['type'] == 'default', 'log_weighted_score'].values[0]
        restaurants_collection.update_one({'_id': rest['_id']}, {'$set': {'Score': restaurant_score}})


def get_nearby_restaurants(location, type_df, point_location):
    data = []
    seen_ids = set()
    average_score = type_df['score'].mean()
    limited_rec = 3*np.log(average_score)
    new_query = restaurants_collection.find({'Location': {'$regex': location, '$options': 'i'},
                                             'Reviews': {'$gte': 4},
                                             'Score': {'$gte': limited_rec}})
    for obj in new_query:
        coordinates = get_geo_coordinates(f"{obj['Street Address']}, {obj['Location']}")
        if coordinates is not None:
            restaurants_collection.update_one({'_id': obj['_id']}, {'$set': {'coordinates': coordinates}})

    nearby_restaurants = restaurants_collection.find(
        {'coordinates':
             {'$near':
                  {'$geometry': point_location,
                   '$maxDistance': 3000  # max distance in meters
                   }
              }
         }
    )
    for restaurant in nearby_restaurants:
        if str(restaurant["_id"]) not in seen_ids:
            data.append(restaurant)
            seen_ids.add(str(restaurant["_id"]))

    sorted_data = sorted(data, key = lambda x: x['Score'], reverse = True)
    return sorted_data
