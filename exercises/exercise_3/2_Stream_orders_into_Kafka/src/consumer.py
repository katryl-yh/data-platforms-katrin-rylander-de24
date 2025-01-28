from quixstreams import Application

app = Application(
    broker_address="localhost:9092", 
    consumer_group="text-splitter", 
    auto_offset_reset="earliest"
    )

jokes_topic = app.topic(name="jokes",
                        value_deserializer="json"
                        )
# sdf = streaming data frame
# lazy evaluation (saves memory) vs eager evaluation
# we save transformations, and we run them all on data when we need it
sdf = app.dataframe(topic=jokes_topic)
# we do not want to apply transformations to big amount of data
# better strategy: mockup data, have a sample data, test your transformations and once you know what had to be don
# then we scale up
# we do not test on actual whole streamed data


# lambda function is anonymous function

# def print_message(message):
#   print(message)
# sdf.update(print_message)
# OR the same but in a shorter way
sdf.update(lambda message: print(f"Input message: {message}"))

# we apply a function, we write a function that transforms the data
# transformation: I want to split the words

def split_words(message):
    return [ {"word" : word} for word in message["joke_text"].split() ]

sdf = sdf.apply(split_words, expand=True)

sdf.update(lambda words: print(f"output: {words}"))

sdf["length"] = sdf["word"].apply(lambda word: len(word))

sdf = sdf.update(lambda row: print(f"Output: {row}"))

if __name__ == "__main__":
    app.run()
