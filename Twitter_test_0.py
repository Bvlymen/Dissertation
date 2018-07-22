import numpy as np
import pandas as pd
import tweepy
import json
import subprocess
import datetime
import sqlite3
import sqlalchemy
import time
import datetime
from requests_oauthlib import OAuth1
import nltk


# create/connect to tweets database:
db_conn = sqlite3.connect('tweeetDB.db')
cur = db_conn.cursor()

#create the function to create the table which to add to
def Create_Table():
	cur.execute("IF NOT EXISTS tweet_store (created_at TEXT, tweet_text TEXT, tweet_favourite_count INTEGER, tweet_retweet_count INTEGER, user_name TEXT, user_screen_name TEXT, user_favourites_count INTEGER, user_followers_count INTEGER, user_friends_count INTEGER, user_id INTEGER, user_created_at TEXT, user_time_since_created REAL, user_listed_count INTEGER, user_location TEXT, vs_compound REAL, vs_neg REAL, vs_neu REAL, vs_pos REAL, tb_polarity REAL, tb_subjectivity REAL ")
	db_conn.commit()

#Initialise sentiment analysers
vs_analyser = nltk.sentiment.vader.SentimentIntensityAnalyzer()
tb_analyser = TextBlob.analyzer

C_Key = 'ILYTd5Abkw85OKTd5sAbSpPdC'
C_Secret='1hHTjqyq5Kitt3b5gka6uPMkvKzuz5wO7C63HcRLP5v2mHUtz6'

A_Token='2758135350-UfWPEgJPQJTCHvCQRQEzcavyl45mcwwLkRnWWBi'
A_Token_Secret='LKa0IlzGhWdLKgzhATUtFx0kM5AGG6lAWBUoOEP1lKj9g'


#verify credentials with twitter first
url = 'https://api.twitter.com/1.1/account/verify_credentials.json'
auth = OAuth1(C_Key, C_Secret, A_Token, A_Token_Secret)
requests.get(url, auth=auth)

#Create the function for inserting data into our table
def Insert_Data():
	#date time at which we collected the tweet
	cur.execute("INSERT INTO tweet_store (created_at, tweet_text, tweet_favourite_count, tweet_retweet_count, user_name, user_screen_name, user_favourites_count, user_followers_count, user_friends_count, user_id, user_created_at, user_time_since_created, user_listed_count, user_location, vs_compound, vs_neg, vs_neu, vs_pos, tb_polarity, tb_subjectivity) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		(datestamp, text, tweet_info['favourite_count'], tweet_info['retweet_count'], user_info['name'], user_info['screen_name'], user_info['favourites_count'], user_info['followers_count'], user_info['friends_count'], user_info['id'], user_info['account_created_at']))
	db_conn.commit()

#Make the request:
#For each Date
for idx, date in enumerate(dates):
	#and each currency
	for currency in crypto_currencies:
	
		response = requests.get('https://api.twitter.com/1.1/tweets/search/fullarchive/Diss0.json?query='+currency+'&fromDate='+dates[idx]+'&toDate='+dates[idx+1]+'&maxResults=20&lang=en',auth= auth)

		data = response.json()

		user_info = {}

		for tweet in data['results']:
			
			text = tweet['text']

			datestamp = tweet['created_at']
			
			tweet_info['retweet_count'] = tweet['retweeted_status']['retweet_count']
			tweet_info['favourite_count'] tweet['retweeted_status']['favourite_count']


			user_info['name'] = tweet['user']['name']
			user_info['screen_name'] = tweet['user']['screen_name']
			user_info['followers_count'] = tweet['user']['followers_count']
			user_info['friends_count'] = tweet['user']['friends_count']
			user_info['favourites_count'] = tweet['user']['favourites_count']
			user_info['statuses_count'] = tweet['user']['statuses_count']
			user_info['id'] = tweet['user']['id']
			user_info['account_created_at'] = tweet['user']['created_at']
			delta_created= time.time()- datetime.datetime.strptime(tweet['user']['created_at'], '%a %b %d %H:%M:%S %z %Y').timestamp()
			user_info['days_since_created'] = datetime.timedelta(seconds = delta_created).days
			user_info['listed_count'] = tweet['user']['listed_count']

			#If a location is available, get it.
			try:
				user_info['location'] = tweet['user']['location']
			except Exception as e:
				user_info['location'] = 'NA'
			
			#Analyse the sentiment of the tweet with vader sentiment
			try:
				vs_sentiment = vs_analyser.polarity_scores(tweet['text'])
				vs_compound = vs_sentiment['compound']
				vs_neu = vs_sentiment['neu']
				vs_neg = vs_sentiment['neg']
				vs_pos = vs_sentiment[pos]

			except Exception as e:
				vs_compound = np.nan
				vs_neu = np.nan
				vs_neg = np.nan
				vs_pos = np.nan

			#analyse the sentiment of the tweet with textblob

			try:
				tb_sentiment = tb_analyser.analyse(tweet['text'])
				#pos/neg
				tb_polarity = tb_sentiment[0]
				#subjectivity/objectivity
				tb_subjectivity = tb_sentiment[1]
			except Exception as e:
				tb_polarity = np.nan
				tb_subjectivity = np.nan




