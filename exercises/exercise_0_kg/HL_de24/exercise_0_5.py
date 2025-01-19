import asyncio
from itertools import count
from random import randint


# async data generator
async def record_generator():  # async declares this as a coroutine function
    c = count(start=1, step=1)
    while True:
        record = f"record-{next(c)}"
        yield "STOP" if randint(1, 8) == 1 else record

        # temporarily give up control
        await asyncio.sleep(1)  # returns control after 1 second


# async iteration of generated records
async def run():
    g = record_generator()
    while True:
        value = await anext(g)  # wait for next value to be ready
        if value == "STOP":
            print("STOP signal encountered. Ending stream processing.")
            break
        print(f"Processed: {value}")


if __name__ == "__main__":
    asyncio.run(run())
    print("\nData stream processing completed. Have a nice day")
