import pika
from customers import new_customers
import time

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

channel.exchange_declare(exchange="my_exchange", exchange_type="direct")

for customer in new_customers:
    time.sleep(2)
    message = f"{customer['name']} visited {customer['restaurant_visited']} at {customer['address']} and gave a score of {customer['score']}"
    print(message)
    channel.basic_publish(exchange="my_exchange", routing_key="", body=message)


# Close the connection
connection.close()

