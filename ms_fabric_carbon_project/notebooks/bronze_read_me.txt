Tento bronzový notebook přestavuje první fázi procesu ETL ke zpracování dat.
Data jsem vybral na základě lehké dostupnosti a veřejnému přistupu a reprezentují odhady a meření emisí uhlíkových splodin ve Velké Británii.

Prvně jsem začal importováním pythnových knihoven které využiji v procesu:

import requests
import json
from datetime import datetime, timedelta, timezone
import os

*datetime sloužilo primárně k uvodnímu nahrátí dat ve větším časovém rozsahu kde jsem zadal manuálně týden:

today = datetime.now(timezone.utc)
yesterday = today - timedelta(days=7)

from_date = yesterday.strftime("%Y-%m-%dT%H:%MZ")
to_date = today.strftime("%Y-%m-%dT%H:%MZ")


url = f"https://api.carbonintensity.org.uk/intensity/{from_date}/{to_date}"


Poté se již ve 24h intervalu nahravají data za posledních 24h kde není potřeba nadefinovat datum:


url = "https://api.carbonintensity.org.uk/intensity/date"

V poslední řadě se vytvoří JSON soubor a pokud již existuje tak se  nová data vyfiltrují pro případné duplikáty které vy mohly potencionálně vzniknout:

response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:

    new_data = response.json().get('data', [])

    file_path = "/lakehouse/default/Files/carbon_emission_UK_data.json"

    # Check if file exists and load existing data
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            try:
                existing_data = json.load(file)
            except json.JSONDecodeError:
                existing_data = []

        # Append only new records by comparing all fields and combine data
        filtered_new_data = [record for record in new_data if record not in existing_data]

        combined_data = existing_data + filtered_new_data

        with open(file_path, 'w') as file:
            json.dump(combined_data, file, indent=4)

        print(f"Appended {len(filtered_new_data)} new records to {file_path}.")
    else:
        # If file doesn't exist, save new data directly
        with open(file_path, 'w') as file:
            json.dump(new_data, file, indent=4)
        print(f"File {file_path} created with {len(new_data)} records.")
else:
    print("Failed to fetch data. Status code:", response.status_code)
