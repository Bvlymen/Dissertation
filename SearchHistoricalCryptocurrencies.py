import numpy as np
import pandas as pd
import tweepy
import json
import subprocess
import datetime
import time
import yaml
import sys


def Search_Twitter_Data_To_File(C_Key, C_Secret, A_Token, A_Token_Secret, Max_Tweets= 100, Filename='C:/Users/Ben/Desktop/Python/Sublime/Dissertation/TwitterData/temp.txt', Filters = ["Bitcoin"], Tweet_Data_Parts = None, *args, **kwargs):

    """
   
    C_Key,
        String - Twitter Consumer Key
    C_Secret, 
        String - Twitter Consumer Secret
    A_Token,
        String - Twitter Access Token
    A_Token_Secret, 
        String - Twitter Access Token Secret
    Max_Tweets= 100,
        INT - Number of tweets to extract
    Filters = ['Bitcoin'], 
        List(String) - What words to filter on
    Filename = 'C:/Users/Ben/Desktop/Python/Sublime/Dissertation/TwitterData/temp.txt',
        String - The name of your new file to append to (default is temp.txt)
    New_Table_Columns = "(date DATETIME, username VARCHAR(20), tweet VARCHAR(280))",
        List(String) - SQL format tuple of string pairs for column name and type e.g. ['time DATETIME', 'age INT(2)']'
    Tweet_Data_Parts = None
        List(String/List(String)) - Parts of the tweet json (according to twitter) to extract e.g. [{"user":"screen_name"}, text'] is default
        Time is automatically added in to database
    
    """
    #Check that file to write to exists
    try:
        with open(Filename,'r') as f:
            print('File specified already exists - Warning that data will be appended here')
    except:
        with open(Filename,'w') as f:
            print('Specified file was created')

    #Set up authetication
    auth = tweepy.OAuthHandler(consumer_key=C_Key, consumer_secret=C_Secret)
    auth.set_access_token(A_Token, A_Token_Secret)
    if Max_Tweets>=3:
        tweet_add_milestone = int(Max_Tweets/5)
    else:
        tweet_add_milestone = 1

     # ## Define a class to listen to the twitter API
    # If we want to use twitter data and/or a database other than the default then define this custom listener:
      
    class Stream_Listener(tweepy.StreamListener):
            def __init__(self, api=None, Max_Tweets_=None, Filename_=None,Tweet_Data_Parts_ = None):
                super().__init__()
                #Store important class attributes
                self.num_tweets = 0
                self.max_tweets = Max_Tweets_        
                self.tweet_data_parts = Tweet_Data_Parts_
                self.filename = Filename_

            def on_data(self, data):
                if self.num_tweets < self.max_tweets:
                    # all_data = json.loads(data)
                    # tweet = unicode(all_data["text"],'utf-8mb4')
                    # username = all_data["user"]["screen_name"]
                    
                    # cur_time = datetime.datetime.strptime(all_data["created_at"], "%a %b %d %H:%M:%S %z %Y")

                    try:
                        with open(self.filename, 'w+',encoding='utf-8') as f:
                            _0 = json.load(f)
                            _1 =json.loads(data)
                            _1['text']=_1['text'].encode('utf-8',errors='replace').decode('utf-8')
                            _0.update({self.num_tweets:_1})
                            json.dump(_0,f, ensure_ascii=False)
                    except:
                        with open(self.filename, 'r+',encoding='utf-8') as f:
                            _1 = json.loads(data)
                            _1['text']=_1['text'].encode('utf-8',errors='replace').decode('utf-8')
                            json.dump({self.num_tweets:_1},f, ensure_ascii=False)

                    
                    if self.num_tweets%tweet_add_milestone == 0 or self.num_tweets ==0:    
                        print("Successfully added tweet. Number:", self.num_tweets +1)
                    self.num_tweets +=1

                    return True
        
                else:
                    self.status_code=200
                    print("Finished writing to file:", self.filename)
                    
                    return False
                     
            def on_error(self, status):
                self.status_code=status
                print("Error Code:", status)


        #Initialise the stream listener
    listener = Stream_Listener(Max_Tweets_ = Max_Tweets, Filename_ = Filename, Tweet_Data_Parts_ = Tweet_Data_Parts)    
    #Authenticate the listener
    data_stream = tweepy.Stream(auth, listener)
    
    #Add filters
    data_stream.filter(track = Filters)

    print("Text File Successfully written to")

    return listener