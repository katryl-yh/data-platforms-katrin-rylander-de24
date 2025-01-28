from quixstreams import Application

# Initialize the Appication
app = Application(
    broker_address="localhost:9092", 
    consumer_group="text-splitter", 
    auto_offset_reset="earliest"
    )

# Define an input topic with JSON deserialization
orders_topic = app.topic(name="orders",
                        value_deserializer="json"
                        )
# sdf = StreamingDataFrame 
# sdf is a lazy object (saves memory) 
# It doesn't store the actual consumed data in memory, 
# but only declares how the data should be transformed.

# Create StreamingDataFrame and connect it to the topic 
sdf = app.dataframe(topic=orders_topic)

# we apply a function, we write a function that transforms the data
# transformation: I want to split the order by product, price and total price

def print_info(message):
    print(f"\nInput: {message} \n")
    total_price = 0
    for element in message["products"]:
        total_price +=element["quantity"]*element["price"]
        print(f"Product: {element["name"]}\t\tQuantity: {element["quantity"]}\tPrice: {element["price"]}" )
        
    print(f"Total price: {total_price}\n")

sdf.update(lambda message: print_info(message))


if __name__ == "__main__":
    app.run()
