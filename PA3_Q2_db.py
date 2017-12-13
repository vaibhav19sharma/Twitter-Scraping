from __future__ import print_function
import tweepy
import json
from pymongo import MongoClient
import pprint
import bson
from textblob import TextBlob

MONGO_HOST = 'mongodb://localhost/usa_db'  # assuming you have mongoDB installed locally
# and a database called 'twitterdb'

WORDS = ['#bigdata', '#deeplearning', '#computervision', '#datascience']

CONSUMER_KEY = " "
CONSUMER_SECRET = " "
ACCESS_TOKEN = " "
ACCESS_TOKEN_SECRET = " "



class StreamListener(tweepy.StreamListener):
    # This is a class provided by tweepy to access the Twitter Streaming API.
    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")
    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print('An Error has occured: ' + repr(status_code))
        return False
    def on_data(self, data):
        try:
            client = MongoClient(MONGO_HOST)
            db = client.usa_db
            datajson = json.loads(data)
            created_at = datajson['created_at']
            coordinates = datajson['coordinates']
            country = datajson['place']['country_code']
            if coordinates and country == "US":
                print("Tweet collected at " + str(created_at), str(datajson['coordinates']))
                db.usa_tweets_collection.insert(datajson)
        except Exception as e:
            print(e)
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True))
streamer = tweepy.Stream(auth=auth, listener=listener)
streamer.filter(locations=[-124.848974, 24.396308, -66.885444, 49.384358])