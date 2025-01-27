import pandas as pd
import json
from pathlib import Path


data_path = Path(__file__).parent 
print(f"\n {data_path} \n")


# Convert JSON string to Pandas DataFrame
df = pd.read_json(data_path / "orders.json")

# Print DataFrame

print(df.head())

print()
print()

print(df["products"].iloc[0] )


