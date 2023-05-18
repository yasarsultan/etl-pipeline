# Remember to activate environment before running this script.
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

# result files
logfile = "logfile.txt"         #file to store all log events
targetfile = "transformed.csv"  #file to store transformed data

# data extracting function for csv file
def extract_csv(file):
    df = pd.read_csv(file)
    return df

# for json file
def extract_json(file):
    df = pd.read_json(file,lines=True)
    return df

# for xml file
def extract_xml(file):
    df = pd.DataFrame(columns=['name', 'height', 'weight'])
    tree = ET.parse(file)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find('height').text)
        weight = float(person.find('weight').text)
        df = df._append({"name":name, "height":height, "weight":weight}, ignore_index = True)
    return df

# Extracting all the files 
def extract():
    extracted_data = pd.DataFrame(columns=['name', 'height', 'weight'])

    for csv in glob.glob('source/*.csv'):
        extracted_data = extracted_data._append(extract_csv(csv), ignore_index = True)
    
    for json in glob.glob('source/*.json'):

        extracted_data = extracted_data._append(extract_json(json), ignore_index = True)

    for xml in glob.glob('source/*.xml'):
        extracted_data = extracted_data._append(extract_xml(xml), ignore_index = True)
    
    return extracted_data

# Transforming data
def transform(dataset):
    # Converting inches to meters *one inch=0.0254 meter
    dataset['height']= round(dataset.height * 0.0254, 2)

    # converting pounds to kilograms 1 pound = 0.45359237 kg
    dataset['weight']= round(dataset.weight * 0.45359237, 2)

    return dataset

# Loading data to a desired file type
def load(target_file, data):
    data.to_csv(target_file)

# Log messages
def log(msg):
    timestamp_format = "%d-%h-%Y  %H-%M"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt", "a") as file:
        file.write(timestamp + ", " + msg + '\n')



log("ETL job started")

log("Extract phase started")
extracted = extract()
log("Extract phase ended")

log("Transform phase started")
transformed = transform(extracted)
log("Transform phase ended")

log("Load phase started")
load(targetfile, transformed)
log("Load phase ended")

log("ETL job ended")