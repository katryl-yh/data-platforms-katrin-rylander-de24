import os
from collections import defaultdict


def parse_network_log(path: str) -> None:
    with open(path) as f:
        data = f.readlines()

    data_transfer = defaultdict(int)

    for row in data:
        stats: dict[str, str] = {}

        for stat in row.split("|")[:-3:-1]:
            key, value = stat.lower().split(":")
            stats[key.strip()] = value.strip()

        data_transfer[stats.get("protocol", "")] += int(stats.get("bytes", 0))

    print("Data Transfer Summary:")
    for key, value in data_transfer.items():
        print(f"{key.upper()}: {value} bytes")


os.chdir(os.path.dirname(__file__))

parse_network_log("data/network.log")
