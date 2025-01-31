import logging
from quixstreams import Application
from uuid import uuid4
from datetime import timedelta
#import pygsheets



def main():
    app = Application(
        broker_address="localhost:9092",
        loglevel="DEBUG",
        consumer_group=str(uuid4()),
        auto_offset_reset="earliest",
    )

    input_topic = app.topic("weather_data_demo")

    sdf = app.dataframe(input_topic)
    
    # sdf = sdf.group_into_hourly_batches()
    sdf = sdf.tumbling_window(duration_ms=timedelta(hours=1))

    # sdf = sdf.summarize_that_hour()
    sdf = sdf.reduce()
    
    # sdf = sdf.send_to_google_Sheets()

    app.run()

if __name__=="__main__":
    logging.basicConfig(level="DEBUG")
    main()