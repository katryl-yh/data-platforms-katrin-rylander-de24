import re 
from pathlib import Path

#print("This is path of this script")
data_path = Path(__file__).parent /"data"

with open(data_path /"ml_text_raw.txt", 'r') as file:
    raw_text = file.read()

text_fixed_spacing = re.sub(r"\s+"," ",raw_text)

#print(text_fixed_spacing)

text_fixed_spacing.split(". ")

sentences = [text.strip().capitalize() for text in text_fixed_spacing.split(".")]
sentences = sentences[:-1]
sentences

cleaned_text = ".\n".join(sentences)
print(cleaned_text)

with open(data_path / "cleaned_ml_text.txt", "w") as file:
    file.write(cleaned_text)


