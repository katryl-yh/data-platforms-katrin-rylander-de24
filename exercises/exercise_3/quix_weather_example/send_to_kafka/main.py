import requests
import json
from quixstreams import Application
import logging
import time

def get_weather():
    response = requests.get("https://api.open-meteo.com/v1/forecast", 
                            params={"latitude":51.5, "longitude":-0.11, "current":"temperature_2m"})
    # checks that what we got as a response is a valid json!
    return response.json() 

# create an application object
def main():
    app = Application(
        broker_address="localhost:9092",
        loglevel="DEBUG"
        )

    # get_producer is a way to establish write-only connection
    # by using with it is python which closes the connection properly and cleans up after it
    with app.get_producer() as producer:
        while True:
            weather = get_weather()
            logging.debug(f"Got weather: {weather}" )
            producer.produce(
                topic="weather_data_demo",
                key="London",
                value=json.dumps(weather) # json file to a json string
            )
            logging.info("Produced. SLeeping...")
            time.sleep(60)

if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    main()