from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from typing import Optional
from restaurants_changes import call_people, restaurants_query, insert_restaurant, restaurants_to_update, restaurants_to_delete
from recommendation_algo import get_location, get_geo_coordinates, get_visited_restaurants, get_visited_types_df, weighted_scores_df, set_scored_restaurants, get_nearby_restaurants


app = FastAPI()


@app.get("/people")  #list comprehension
async def show_people():
    data = call_people()
    return data


@app.get("/restaurants")  #list comprehension
async def show_restaurants(name: Optional[str] = None, state: Optional[str] = None, min_score: Optional[float] = None):
    query = {}
    if name:
        query["Name"] = {'$regex': name, "$options": "i"}
    if state:
        query["Location"] = {'$regex': state, "$options": "i"}
    if min_score is not None:
        query["Reviews"] = {'$gte': min_score}
    return restaurants_query(query)


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

    insert_restaurant(restaurant_to_add)


class UpdateRestaurantDetails(BaseModel):
    Name: Optional[str] = Field(None)
    Street_Address: Optional[str] = Field(None)
    Location: Optional[str] = Field(None)
    Type: Optional[str] = Field(None)
    Comments: Optional[str] = Field(None)
    Contact_Number: Optional[str] = Field(None)


@app.put("/restaurant/update_restaurant")
async def update_restaurant_details(restaurant_name: str, address: str, restaurant_details: UpdateRestaurantDetails):
    response = restaurants_to_update(restaurant_name, address, restaurant_details)
    if response.matched_count:
        return {"message": f"Restaurant '{restaurant_name}' details updated successfully."}
    else:
        raise HTTPException(status_code = 404, detail = f"Restaurant '{restaurant_name}' not found.")


@app.delete("/restaurant/delete_restaurant", status_code = status.HTTP_204_NO_CONTENT)
async def delete_restaurant(restaurant_name: str, address: Optional[str] = None):
    response = restaurants_to_delete(restaurant_name, address)
    if response.deleted_count:
        return {"message": f"Restaurant {restaurant_name} at {address} deleted."}
    else:
        raise HTTPException(status_code = 404, detail = f"Restaurant {restaurant_name} at {address} not found")


@app.get("/restaurant/recommendation/{full_name}/{state}/{city}")
async def recommend_restaurant(full_name: str, state: str, city: str, street: Optional[str] = None):
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



