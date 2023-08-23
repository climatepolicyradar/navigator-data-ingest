import os
import json

# Specify the directory containing the JSON files
directory_path = "integration_tests/data/pipeline_out"

# Iterate over each file in the directory tree
for root, _, files in os.walk(directory_path):
    for filename in files:
        if filename.endswith(".json"):
            file_path = os.path.join(root, filename)

            # Load the JSON content from the file
            with open(file_path, "r") as json_file:
                try:
                    json_data = json.load(json_file)
                except json.JSONDecodeError:
                    print(f"Error parsing JSON in file: {file_path}")
                    continue

            # Write the JSON content back to the file with correct indentation
            with open(file_path, "w") as json_file:
                json.dump(
                    json_data, json_file, indent=2
                )  # Adjust the indent value as needed
