from __future__ import print_function
import tweepy
import json
from pymongo import MongoClient
import pprint
import bson
from textblob import TextBlob

MONGO_HOST = 'mongodb://localhost/twitterdb'  # assuming you have mongoDB installed locally
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
        # This is the meat of the script...it connects to your mongoDB and stores the tweet
        try:
            client = MongoClient(MONGO_HOST)

            # Use twitterdb database. If it doesn't exist, it will be created.
            db = client.twitterdb

            # Decode the JSON from Twitter
            datajson = json.loads(data)

            # grab the 'created_at' data from the Tweet to use for display
            created_at = datajson['created_at']

            # print out a message to the screen that we have collected a tweet
            print("Tweet collected at " + str(created_at))

            # insert the data into the mongoDB into a collection called twitter_search
            # if twitter_search doesn't exist, it will be created.
            db.twitter_search.insert(datajson)
        except Exception as e:
            print(e)


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
# Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True))
streamer = tweepy.Stream(auth=auth, listener=listener)
print("Tracking: " + str(WORDS))
#streamer.filter(locations=[-124.848974, 24.396308,-66.885444, 49.384358])
client = MongoClient(MONGO_HOST)
db = client.twitterdb
doc = db.twitter_search.aggregate([{'$match':{'$or':[{'extended_tweet.full_text':{'$regex':".*data.*"}},{'text':{'$regex':".*data.*"}}]}},{'$group':{'_id':'$id'}},{'$count':"Tweets"}])
print("QUESTION 1: PART A")
for i in doc:
    pprint.pprint(i)

print("QUESTION 1: PART B")
p2= db.twitter_search.find({"$and":[{'$or':[{'extended_tweet.full_text':{'$regex':".*data.*"}},{'text':{'$regex':".*data.*"}}]},{"user.geo_enabled":{"$ne": 'false'}}]}).count()
print("Number of tweets in data with Geo-enabled and 'data' keyword are: {}".format(p2))

print("QUESTION 1: PART C")
p3= db.twitter_search.find({'$or':[{'extended_tweet.full_text':{'$regex':".*data.*"}},{'text':{'$regex':".*data.*"}}]},{'extended_tweet.full_text':1,'text':1,'_id':0})
k=0
for i in p3:
    if 'extended_tweet' in i:
        tweet = i['extended_tweet']['full_text']
    else:
        tweet = i['text']
    text = TextBlob(tweet)
    if (text.sentiment.polarity < 0):
        print(" **** Positive sentiment for the tweet **** " + tweet)
    elif (text.sentiment.polarity == 0):
        print("**** Neutral sentiment for the tweet **** " + tweet)
    else:
        print(" **** Negative sentiment for the tweet **** " + tweet)
    k=k+1
print(k)