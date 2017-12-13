Prerequisites:
1. Python-3
2. Mongodb


Head over to https://apps.twitter.com/ and create a new application.
Once you've created a new application look out for the Keys and Access Tokens. These will be used to request twitter for real time data.

Open the python file and fill out the 4 access tokens.

PA3_Q1_db scrapes real time data based on hashtags. If you wish to change the hashtags, search for WORDS and add/delete the hashtags you wish to scrape.

PA3 does the following:
1) Finds the number of tweets that have *data* somewhere in the tweet’s text
2) For all the data related tweets, tells if the tweet is positive, neutral or negative sentiment. This is found out using TextBlod library.

PA3_Q2_db scrapes all real time data confining the bounding box that is enclosed by USA. Also, some part of Mexico, Bahamas and Canada is present in this. To specifically scrape US based tweets, I have changed the country to US.

PA3_Q2_db does the following:
1) Finds top 15 emojis in the all the tweets
2) Top 5 states with the Christmas Tree emoji (🎄)
3) Top 5 states that use emojis
4) Top 5 states that have tweets
5) Top 5 California cities that tweet
6) Creates a Map of US showing the origin of tweets(check output_Question4.html)
7) Shows top 2 emojis per state of US(check output_q4_extra.html)
