import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"

# ---------- Extraction Functions ----------
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe

def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for car in root:
        car_model = car.find("car_model")
        year = car.find("year_of_manufacture")
        price = car.find("price")
        fuel = car.find("fuel")

        # Check if elements exist and extract text safely, else assign None or default
        car_model = car_model.text if car_model is not None else None
        year = int(year.text) if (year is not None and year.text.isdigit()) else None
        try:
            price = float(price.text) if price is not None else None
        except (ValueError, TypeError):
            price = None
        fuel = fuel.text if fuel is not None else None

        dataframe = pd.concat([
            dataframe,
            pd.DataFrame([{
                "car_model": car_model,
                "year_of_manufacture": year,
                "price": price,
                "fuel": fuel
            }])
        ], ignore_index=True)
    return dataframe


def extract():
    extracted_data = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])

    # Extract from CSV
    for csvfile in glob.glob("*.csv"):
        if csvfile != target_file:
            extracted_data = pd.concat([extracted_data, extract_from_csv(csvfile)], ignore_index=True)

    # Extract from JSON
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, extract_from_json(jsonfile)], ignore_index=True)

    # Extract from XML
    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, extract_from_xml(xmlfile)], ignore_index=True)

    return extracted_data

# ---------- Transformation Function ----------
def transform(data):
    # Convert 'price' to numeric, invalid parsing will be set as NaN
    data['price'] = pd.to_numeric(data['price'], errors='coerce')

    # Round prices to 2 decimals (NaN will stay NaN)
    data['price'] = data['price'].round(2)

    return data


# ---------- Load Function ----------
def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file, index=False)

# ---------- Logging Function ----------
def log_progress(message):
    timestamp_format = '%Y-%b-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')

# ---------- Main ETL Flow ----------
log_progress("ETL Job Started")

log_progress("Extract phase Started")
extracted_data = extract()
log_progress("Extract phase Ended")

log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
log_progress("Transform phase Ended")

log_progress("Load phase Started")
load_data(target_file, transformed_data)
log_progress("Load phase Ended")

log_progress("ETL Job Ended")

# Optional: preview output
print("✅ ETL job completed.")
print(f"Rows processed: {len(transformed_data)}")
