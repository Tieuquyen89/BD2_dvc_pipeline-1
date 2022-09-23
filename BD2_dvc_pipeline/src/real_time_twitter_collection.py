# Import required packages
import tweepy
import pandas as pd
import configparser
from pymongo import MongoClient
from datetime import date, datetime
from time import sleep
import logging
import os


# Function to collect streaming data
def twitter_collection_data(config):
    
    #Twitter api key and key secret
    api_key = config['twitter']['api_key']
    api_key_secret = config['twitter']['api_key_secret']
    #Twitter access token and token secret
    access_token = config['twitter']['access_token']
    access_token_secret = config['twitter']['access_token_secret']

    #Number of tweets limit and number of runs limit
    number_of_tweets = config['data_extraction']['number_of_tweets']
    number_of_runs = config['data_extraction']['number_of_runs']
    
    #Mongo DB storage details
    url = config['data_storage']['url']
    db = config['data_storage']['db']
    collection = config['data_storage']['collection']
    
    #Authenticate to Twitter API
    auth = tweepy.OAuthHandler(api_key, api_key_secret)
    auth.set_access_token(access_token, access_token_secret)

    # MongoDB connection 
    myclient = MongoClient(url)
    mydb = myclient[db]
    mycol = mydb[collection]
	
    # Count of documents in collection before insert
    count_before = mycol.count_documents({})
    logging.info("Count of documents in collection before insert : " + str(count_before))
    
    #Create API
    api = tweepy.API(auth, wait_on_rate_limit=True)

    tweets_df = []
    time_df = []
    user_df = []
	
    try:
        # Streaming data
        for i in range(int(number_of_runs)):
            for tweet in tweepy.Cursor(api.home_timeline, tweet_mode = "extended").items(int(number_of_tweets)):
                tweets_df.append(tweet.full_text)
                time_df.append(tweet.created_at)
                user_df.append(tweet.user.screen_name)
                tweets = tweet.full_text
                time = tweet.created_at
                user= tweet.user.screen_name
               
                # Code sleeping to avoid over limitation
                async def main():
                    await asyncio.sleep(900)
                
                # Dictionary format to insert to Mongo
                title_list = ["Tweets", "Created At", "User"]
                tweets_list = [tweets, time, user]
                data_dictionary = dict(zip(title_list, tweets_list))
                
                # Insert into database
                result = mycol.insert_one(data_dictionary)
                logging.info("Inserted record : " + str(result.inserted_id))
        
        msg = "Insert successful !"
    except Exception as ex:
        msg="Error while inserting records in MongoDB!"
        logging.error("Error while inserting records in MongoDB! " + str(ex))
        return ex
           
    return msg

   
#Read data (data got until today from Mongo DB)
def fetch_data_from_Mongo(config):
	
	#Mongo DB storage details
    url = config['data_storage']['url']
    db = config['data_storage']['db']
    collection = config['data_storage']['collection']
	
	#Connect to DB 
    client = MongoClient(url)
    records = client.get_database(db)[collection]
    
    #Fetch all records
    df = pd.DataFrame.from_dict(records.find())
    
    return df


#Read credentials from config file
def read_config_file():

    config = configparser.ConfigParser()  #creating a config parser instance
    config.read('/home/hemap/big-data-programming-2-april-2021-haaq-alwaysexecutable/src/config.ini')
    
    return config
   
def main():
    
    start = datetime.now()
	
	# Set change directory path
    cwd = os.getcwd()
    parent_dir = os.path.abspath(os.path.join(cwd, os.pardir))
    
    # Configuring logging to log the current run information
    # logs_dir = os.path.join(parent_dir,'logs')
    logs_dir = '/home/hemap/big-data-programming-2-april-2021-haaq-alwaysexecutable/logs'
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    log_file_name = 'logs_data_extraction_'+str(date.today())+'.log'
    logs_file = os.path.join(logs_dir, log_file_name)
    
    # Configuring the File mane to logging Level
    logging.basicConfig(filename=logs_file,level=logging.INFO)
    
    #Read config file for data extraction configuration
    config = read_config_file()
    
    #Collect streaming tweets 
    logging.info("Data extraction starts : "+ str(start))
    msg = twitter_collection_data(config)
    logging.info("Data extracted successfully!")
	
    #Fetch updated data records until today from Monngo DB collection
    df=fetch_data_from_Mongo(config)
    
    #Save fetched data to csv format in data folder
    #data_dir = os.path.join(parent_dir,'data')
    data_dir = '/home/hemap/big-data-programming-2-april-2021-haaq-alwaysexecutable/data'
    if not os.path.exists(logs_dir):
        os.makedirs(data_dir)
    data_file = os.path.join(data_dir, 'twitter_data.csv')
    df.to_csv(data_file,index=False)
    logging.info("Data stored successfully!")
    
    end = datetime.now()
    logging.info("Time taken : "+ str(end-start))
    
    return None
    
if __name__ == "__main__":
	
    main()
    
