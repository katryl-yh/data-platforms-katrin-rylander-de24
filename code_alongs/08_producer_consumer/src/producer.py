from pathlib import Path
import json
from pprint import pprint

data_path = Path(__file__).parents[1] / "data"
print()
print(data_path)
print()

with open(data_path/ "jokes.json", "r") as file:
    jokes = json.load(file)

pprint(jokes)