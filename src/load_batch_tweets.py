# Import required packages
import pandas as pd
import configparser
import json
from pymongo import MongoClient
from datetime import date, datetime
from time import sleep
import logging
import os


# Function to import batch data from csv to Mongo DB collection
def load_batch_tweets_to_mongo(config, batch_data_path):
    
    #Mongo DB storage details
    url = config['data_storage']['url']
    db = config['data_storage']['db']
    collection = config['batch_data_load']['collection']

    # MongoDB connection 
    myclient = MongoClient(url)
    mydb = myclient[db]
    mycol = mydb[collection]

    tweets_df = []
    time_df = []
    user_df = []
    try:
        data = pd.read_csv(batch_data_path)
        payload = json.loads(data.to_json(orient='records'))
        mycol.drop()
        mycol.insert_many(payload)
        count = mycol.count_documents({})
        logging.info("Inserted "+ str(count)+ " records into "+str(config['batch_data_load']['collection']))
    except Exception as ex:
        logging.error("Error while inserting records in MongoDB! \n" + str(ex))
	           
    return None

#Read credentials from config file
def read_config_file():

    config = configparser.ConfigParser()  #creating a config parser instance
    config.read('config.ini')
    
    return config

#Mainline method
def main():
    
    start = datetime.now()
    
    # Set change directory path
    cwd = os.getcwd()
    parent_dir = os.path.abspath(os.path.join(cwd, os.pardir))
    
    # Configuring logging to log the current run information
    logs_dir = os.path.join(parent_dir,'logs')
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    log_file_name = 'logs_batch_data_load.log'
    logs_file = os.path.join(logs_dir, log_file_name)
        
    # Configuring the File mane to logging Level
    logging.basicConfig(filename=logs_file,level=logging.INFO)
    logging.info("Batch data loading starts!")
    
    #Read config file for data extraction configuration
    config = read_config_file()
    
    #Load batch tweets to DB 
    batch_data_path = os.path.join(parent_dir,'data/batch_tweets.csv')
    msg = load_batch_tweets_to_mongo(config, batch_data_path)
    logging.info("Batch data loaded successfully!")
    
    end =  datetime.now()
    logging.info("Time taken :" + str(end-start))
    
    return None
    
if __name__ == "__main__":
	
    main()

