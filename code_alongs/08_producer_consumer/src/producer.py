from pathlib import Path
import json
from  quixstreams import Application
from pprint import pprint

data_path = Path(__file__).parents[1] / "data"
print()
print(data_path)
print()

with open(data_path/ "jokes.json", "r", encoding="utf-8") as file:
    jokes = json.load(file)

#pprint(jokes)
print()
# consumer group allows parallel processing
app = Application(broker_address="localhost:9092", consumer_group="text-splitter")

# topic with jokes in json format
jokes_topic = app.topic(name="jokes", value_serializer="json")

#print(jokes_topic)

def main():
    with app.get_producer() as producer:
        # print(producer)

        for joke in jokes:
            kafka_msg = jokes_topic.serialize(key=joke["joke_id"], value=joke)
            print(f"\nProduced message: key = {kafka_msg.key},value = {kafka_msg.value}")

            producer.produce(
                topic="jokes", 
                key= str(kafka_msg.key), 
                value= kafka_msg.value
            )

# run this code only when executing this script not when importing this module
if __name__ == '__main__':
    # pprint(jokes)
    main()
