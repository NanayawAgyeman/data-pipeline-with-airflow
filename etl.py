import pandas as pd 
import tweepy
import json
from datetime import datetime
import s3fs
import os
from dotenv import load_dotenv

def run_twitter_etl():
    load_dotenv()

    access_key = os.getenv('Access_Token')
    access_secret = os.getenv('Access_Token_Secret')
    consumer_key = os.getenv('API_Key')
    consumer_secret = os.getenv('API_Secret')

    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    # Creating an API object 
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk', 
                               count=200,
                               include_rts=False,
                               tweet_mode='extended')

    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                         'text': text,
                         'favorite_count': tweet.favorite_count,
                         'retweet_count': tweet.retweet_count,
                         'created_at': tweet.created_at}

        tweet_list.append(refined_tweet)

    df = pd.DataFrame(tweet_list)
    df.to_csv('refined_tweets.csv', index=False)

if __name__ == "__main__":
    run_twitter_etl()
