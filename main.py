import os
import json
import time
import subprocess

# Define the path to the JSON file
json_file_path = 'user.json'
script_to_run = '/Users/emerybosc/Documents/oro_game/game_extract.py' 
# Function to get the current modification timestamp of the JSON file
def get_file_modification_timestamp(file_path):
    try:
        return os.path.getmtime(file_path)
    except OSError:
        return None

# Function to load JSON data from the file
def load_json(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except (OSError, json.JSONDecodeError):
        return None

# Function to check if the JSON file has been modified
def is_json_modified(file_path, last_modification_time):
    current_modification_time = get_file_modification_timestamp(file_path)

    if current_modification_time is None:
        return False  # File doesn't exist or cannot be accessed
    elif current_modification_time > last_modification_time:
        return True  # File has been modified
    else:
        return False  # File has not been modified

# Initial check
last_modification_time = get_file_modification_timestamp(json_file_path)
if last_modification_time is None:
    print("The JSON file doesn't exist or cannot be accessed.")
else:
    while True:
        if is_json_modified(json_file_path, last_modification_time):
            print("The JSON file has been modified.")
             # Execute the script when the JSON file is modified
            subprocess.run(['python', script_to_run])
            
            last_modification_time = get_file_modification_timestamp(json_file_path)
        else:
            print("The JSON file has not been modified.")
        time.sleep(5)  # Check every 5 seconds
