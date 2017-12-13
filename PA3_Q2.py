import pymongo
import pprint
import emoji
from textblob import TextBlob
import re
import operator
import folium
import collections
import pandas as pd
from pymongo import MongoClient
MONGO_HOST = 'mongodb://localhost/usa_db'
client = MongoClient(MONGO_HOST)
db = client.usa_db
collection = db.usa_tweets_collection
doc_emojis= collection.find({"$or":[{"place.place_type":"city"},{"place.place_type":"neighborhood"}]},{"text":1,"extended_tweet.full_text":1,"place.full_name":1})
doc = doc_emojis
count = 0
dict_tweet = dict()
e2={}
christmasQ= {}
#for christmasTree Tweet
christmasTree= '🎄'
state = 'MA'
q3= {}
for tweet in doc_emojis:
    if 'extended_tweet' in tweet:
        analyse = tweet['extended_tweet']['full_text']
    else:
        analyse=tweet['text']
    for char in analyse:
        if char in emoji.UNICODE_EMOJI:
            st = tweet['place']['full_name'][-2:]
            if char in e2:
                if st in e2[char]:
                    e2[char][st] = e2[char][st]+1
            else:
                e2[char] = dict()
                e2[char][st]=1
            count=count+1
            dict_tweet[count] = tweet
            if state in tweet['place']['full_name']:
                for char in analyse:
                    if char in emoji.UNICODE_EMOJI:
                        if char in q3:
                            q3[char] = q3[char] + 1
                        else:
                            q3[char] = 1
            break


e1={}
doc_emoji= collection.find({"$or":[{"place.place_type":"city"},{"place.place_type":"neighborhood"}]},{"text":1,"extended_tweet.full_text":1,"place.full_name":1})

for c in doc_emoji:
    strip = c['place']['full_name']
    strip = strip[-2:]
    if 'extended_tweet' in c:
        analyse = c['extended_tweet']['full_text']
    else:
        analyse=c['text']
    for char in analyse:
        if char in emoji.UNICODE_EMOJI:
            if strip in e1:
                e1[strip] = e1[strip] + 1
            else:
                e1[strip] = 1



emjCount={}
for key,value in e2.items():
    if key not in emjCount:
        for k,v in value.items():
            emjCount[key] = 0
            emjCount[key] += v
    else:
        for k2,v2 in value.items():
            emjCount[key] += v2

doc_emoji1= collection.find({"$or":[{"place.place_type":"city"},{"place.place_type":"neighborhood"}]},{"text":1,"extended_tweet.full_text":1,"place.full_name":1})
d=0
for c in doc_emoji1:
    if 'extended_tweet' in c:
        analyse = c['extended_tweet']['full_text']
    else:
        analyse = c['text']
    for char in analyse:
        if char == christmasTree:
            st = c['place']['full_name'][-2:]
            if st in christmasQ:
                christmasQ[st] = christmasQ[st]+1
            else:
                christmasQ[st]=1


print("QUESTION 2: Question 1")
sorted_q1 = sorted(emjCount.items(), key=operator.itemgetter(1))
top15=sorted_q1[-15:]
print(top15[::-1])

print("QUESTION B: Question 2")
sor3 = sorted(christmasQ.items(), key=operator.itemgetter(1))
ch5 = sor3[-5:]
print(ch5[::-1])

print("QUESTION B: Question 3")
print("FOR MA:")
sorted_q3 = sorted(q3.items(), key=operator.itemgetter(1))
top15MA= sorted_q3[-5:]
print(top15MA[::-1])

print("QUESTION B: Question 4")
sorted_q2 = sorted(e1.items(), key=operator.itemgetter(1))
topStates = sorted_q2[-5:]
print(topStates[::-1])

print("QUESTION C: PART A")

Q3= collection.aggregate([{'$match':{"$or":[{"place.place_type":"city"},{"place.place_type":"neighborhood"}]}},{'$project':{"city_state":{'$split' : ['$place.full_name', ', ']}}},{'$unwind': '$city_state'},{'$match':{'city_state':{'$regex':'[A-Z][A-Z]'}}},{'$group':{'_id':{'state':'$city_state'},'total':{'$sum':1}}},{'$sort':{"total": -1}},{'$limit':5}])
for i in Q3:
    print(i)
print("QUESTION C:PART B")
Q4= collection.aggregate([{'$match':{"$or":[{"place.place_type":"city"},{"place.place_type":"neighborhood"}]}},{'$project':{"city_state":{'$split' : ['$place.full_name', ', ']},'place.name':1}},{'$unwind': '$city_state'},{'$match':{'city_state':{'$regex':'[A-Z][A-Z]'}}},{'$match':{'city_state':'CA'}},{'$group':{'_id':{'city':'$place.name'},'total':{'$sum':1}}},{'$sort':{"total": -1}},{'$limit':5}])
for i in Q4:
    print(i)


print("QUESTION D")
GDP = pd.read_csv('usa_tweets.csv')
m = folium.Map(location = [43,-94],zoom_start=6)
for e in GDP.iterrows():
    folium.CircleMarker(location=[e[1]['lat'],e[1]['long']]).add_to(m)

m.save('output_Question4.html')

print("PART D: EXTRA QUESTION")
p4 = doc_emojis= collection.find({"$or":[{"place.place_type":"city"},{"place.place_type":"neighborhood"}]},{"text":1,"extended_tweet.full_text":1,"place.full_name":1})
extra={}
for c in p4:
    strip = c['place']['full_name']
    strip = strip[-2:]
    if 'extended_tweet' in c:
        analyse = c['extended_tweet']['full_text']
    else:
        analyse=c['text']
    for char in analyse:
        if char in emoji.UNICODE_EMOJI:
            if strip in extra:
                if char in extra[strip]:
                    extra[strip][char] = extra[strip][char]+1
                else:
                    extra[strip][char] = 1
            else:
                extra[strip]=dict()
                extra[strip][char]= 1

# df_final = pd.DataFrame(index=index, columns=columns)
# df = pd.DataFrame(columns=['State','lat','long','emoji','count'])
new= dict()

for k,v in extra.items():
    sorted_q4 = sorted(v.items(), key=operator.itemgetter(1))
    print(k,sorted_q4[-2:])
    if k not in new:
        new[k] = sorted_q4[-2:]


for k,v in new.items():
    print(k,v)

latLong = pd.read_csv('zip_codes_states.csv')
latLong['emoji'] = pd.Series()
latLong['emoji']=' '
print(latLong)
for i,row in latLong.iterrows():
    if row['state'] in new:
        latLong.set_value(i,'emoji',new[row['state']])
print(latLong)
# latLong['emoji']= latLong['emoji'].fillna("")


q4extra = folium.Map(location = [43,-94],zoom_start=5)
for e in latLong.iterrows():
    if e[1]['lat'] != 'blah':
        temp = ((e[1]['emoji']))
        if len(temp) == 2:
            str = temp[0][0] + temp[1][0]
        else:
            str = temp[0][0]
        folium.Marker(location=[e[1]['lat'],e[1]['long']],popup =str).add_to(q4extra)

q4extra.save('output_q4_extra.html')