import pika
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["project1"]
restaurants_collection = db["Restaurants"]
people_collection = db["people"]

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

channel.exchange_declare(exchange = "my_exchange", exchange_type = "direct")

restaurants_queue = channel.queue_declare(queue = "Restaurants", exclusive = False).method.queue
people_queue = channel.queue_declare(queue = "people", exclusive = False).method.queue
channel.queue_bind(exchange = "my_exchange", queue = restaurants_queue, routing_key = "")
channel.queue_bind(exchange = "my_exchange", queue = people_queue, routing_key = "")


def restaurant_callback(ch, method, properties, body):
    print(body)
    message_parts = body.decode().split(" visited ")
    # person_name = message_parts[0]
    rest_of_parts = message_parts[1].split(" at ")
    restaurant_name = rest_of_parts[0]
    address = rest_of_parts[1].split(" and gave a score of ")[0]
    score = float(rest_of_parts[1].split(" and gave a score of ")[1])

    # check if restaurant already exist:
    add_restaurant = restaurants_collection.find_one({'Name': restaurant_name, 'Street Address': address})
    if add_restaurant:
        current_score = add_restaurant['Reviews']
        current_num_of_reviews = add_restaurant['No of Reviews']
        new_average_score = ((current_score * current_num_of_reviews) + score) / (current_num_of_reviews + 1)
        restaurants_collection.update_one({'Name': restaurant_name, 'Street Address': address},
                                          {'$set': {'Reviews': new_average_score},
                                           '$inc': {'No of Reviews': 1}})
    else:
        restaurants_collection.insert_one({'Name': restaurant_name,
                                           'Street Address': address,
                                           'Reviews': score,
                                           'No of Reviews': 1})

    # Acknowledge the message
    ch.basic_ack(delivery_tag = method.delivery_tag)


people_collection.update_many({}, [{'$set': {'restaurant_visited': [{'restaurant': '$restaurant_visited',
                                                                     'score': '$score'}]}}])


def people_callback(ch, method, properties, body):
    print(body)
    # Extract data from the message
    message_parts = body.decode().split(" visited ")
    person_name = message_parts[0]
    rest_of_parts = message_parts[1].split(" at ")
    restaurant_name = rest_of_parts[0]
    # address = rest_of_parts[1].split(" and gave a score of ")[0]
    score = float(rest_of_parts[1].split(" and gave a score of ")[1])

    add_person = people_collection.find_one({'name': person_name})

    restaurant_dict = {'restaurant': restaurant_name, 'score': score}
    if add_person:
        people_collection.update_one({'name': person_name},
                                     {'$push': {'restaurant_visited': restaurant_dict}})
    else:
        people_collection.insert_one({'name': person_name,
                                      'restaurant_visited': [restaurant_dict]})

    ch.basic_ack(delivery_tag = method.delivery_tag)


channel.basic_consume(queue = restaurants_queue, on_message_callback = restaurant_callback)
channel.basic_consume(queue = people_queue, on_message_callback = people_callback)
channel.start_consuming()

connection.close()