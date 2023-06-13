from pymongo import MongoClient
import os
from geopy import Bing
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from collections import defaultdict

client = MongoClient("mongodb://localhost:27017/")
db = client["project1"]
restaurants_collection = db["Restaurants"]
people_collection = db["people"]
restaurants_collection.create_index([("coordinates", "2dsphere")])


load_dotenv()  # take environment variables from .env.
API_KEY = os.getenv('BING_API_KEY')
geolocator = Bing(api_key = 'AhpwrCFQAHjj_6XelmahpGUxECXe1tonsrdoV2zAc9VJETqAj-6ekcmoMaKv5Ri6', timeout = 10)


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


def set_scored_restaurants(location, dataframe): # todo default dict
    default_dict = defaultdict(
        lambda: dataframe.loc[dataframe['type'] == 'default', 'log_weighted_score'].values[0],
        {row.type: row.log_weighted_score for row in dataframe.itertuples()}
    )
    query = restaurants_collection.find({'Location': {'$regex': location, '$options': 'i'},
                                         'Reviews': {'$gte': 4}})
    for rest in query:
        restaurant_score = 0
        types = rest['Type'].split(', ')
        for t in types:
            restaurant_score += default_dict[t]
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
