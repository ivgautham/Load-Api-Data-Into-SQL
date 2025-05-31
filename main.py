import requests
from sqlalchemy import create_engine
import pandas as pd
import os

#SQL Connection Credentials
username = "root"
password = "root"
server   = "localhost"
port     = "3306"
database = "sakila"

base_url = "https://pokeapi.co/api/v2/"

def extract(name):
    url = f"{base_url}/pokemon/{name}"
    response = requests.get(url, verify=False)
    print(response)

    if response.status_code == 200:
        data = response.json()
        transform(data)
    else:
        print(f"Failed to retrieve data {response.status_code}")

def transform(data):
    # Convert JSON to DataFrame
    pokemon = {
            "id": data["id"],
            "name": data["name"],
            "base_experience": data["base_experience"],
            "height": data["height"],
            "weight": data["weight"],
            "types": ', '.join([t["type"]["name"] for t in data["types"]]),
            "abilities": ', '.join([a["ability"]["name"] for a in data["abilities"]]),
            "sprite_url": data["sprites"]["front_default"]
        }
    df = pd.DataFrame([pokemon])
    load(df)

def load(df):
    try:
        rows_imported = 0
        table_name = "pokemon"
        #To create an sql connection
        engine = create_engine(f'mysql+mysqlconnector://{username}:{password}@{server}:{port}/{database}')
        print("Connected to MYSQL successfully!",engine)
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... ')
        # save df to database
        df.to_sql(f"{table_name}", engine, if_exists='replace', index=False)
        rows_imported += len(df)
        print("Data imported successfully!")
    except Exception as e:
        print(f"Error: {e}")

pokemon_name = "pikachu"
pokemon_data = extract(pokemon_name)
