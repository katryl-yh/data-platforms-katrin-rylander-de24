from pathlib import Path
import json
from  quixstreams import Application
from pprint import pprint

data_path = Path(__file__).parents[1] / "data"
#print(f"\n{data_path= }\n")

# read in orders, use the absolute path to the file
with open(data_path/ "orders.json", "r", encoding="utf-8") as file:
    orders = json.load(file)

#print(f"Type of orders: {type(orders)}")

# localhost:9092 is a port mapped to broker container
# it only works when broker is up and running
# server must be started locally

# step 1. Create an Application
app = Application(
    broker_address="localhost:9092", # Kafka broker address
    consumer_group="text-splitter" # Kafka consumer group
    )

# value_serializer specifies how the value (i.e., the message content) 
# should be serialized before sending it to the topic.
# In this case, 'json' indicates that the message's value should be serialized into JSON format
# before being sent. 
# This ensures that the message can be properly interpreted by the consumer, 
# as they will also expect the data to be in JSON format.

# Step 2. Define a Topic and serialization
order_topic = app.topic(name="orders", value_serializer="json")

# Step 3. Create a Producer and produce messages
def main():
    with app.get_producer() as producer:

        for order in orders: # we loop though a list of dictionaries
            kafka_msg = order_topic.serialize(key=order["order_id"], value=order)
            print(f"\n\nProduced message: \nkey = {kafka_msg.key},\nvalue = {kafka_msg.value}")

            producer.produce(
                topic="orders", 
                key= str(kafka_msg.key), 
                value= kafka_msg.value
            )

# run this code only when executing this script not when importing this module
if __name__ == '__main__':
    main()
