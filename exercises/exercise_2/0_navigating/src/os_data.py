import sys
import importlib.metadata

from pathlib import Path

# Print the Python version
print("\n\n")
print(f"Python version: {sys.version}")

# List all installed packages using importlib.metadata
print("\nInstalled packages:")
for package in importlib.metadata.distributions():
    print(f"{package.metadata['Name']}=={package.metadata['Version']}")
