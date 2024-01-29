# Importing necessary libraries
import pandas as pd 
import tweepy
import json
from datetime import datetime
import s3fs
import os
from dotenv import load_dotenv

def run_twitter_etl():
    """
    Function to perform ETL (Extract, Transform, Load) process on Twitter data of a specified user.

    The function extracts tweets from a specified Twitter user's timeline, refines the data,
    and stores it in a CSV file.

    Requirements:
    - Tweepy library for Twitter API interaction.
    - Pandas for data manipulation.
    - s3fs for working with Amazon S3 (though not used in the provided script).
    - dotenv for loading environment variables.

    Environment Variables:
    - Access_Token: Twitter access token.
    - Access_Token_Secret: Twitter access token secret.
    - API_Key: Twitter API key.
    - API_Secret: Twitter API secret key.

    Output:
    - A CSV file named 'refined_tweets.csv' containing refined Twitter data.

    Note: Ensure that the required environment variables are set before running the script.
    """
    # Load environment variables from a .env file
    load_dotenv()

    # Retrieve Twitter API credentials from environment variables
    access_key = os.getenv('Access_Token')
    access_secret = os.getenv('Access_Token_Secret')
    consumer_key = os.getenv('API_Key')
    consumer_secret = os.getenv('API_Secret')

    # Twitter authentication using Tweepy
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    # Creating an API object using Tweepy
    api = tweepy.API(auth)

    # Retrieve recent tweets from a specified user's timeline
    tweets = api.user_timeline(screen_name='@elonmusk', 
                               count=200,
                               include_rts=False,
                               tweet_mode='extended')

    # Extract relevant information from each tweet and store in a list
    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {
            "user": tweet.user.screen_name,
            'text': text,
            'favorite_count': tweet.favorite_count,
            'retweet_count': tweet.retweet_count,
            'created_at': tweet.created_at
        }

        tweet_list.append(refined_tweet)

    # Create a Pandas DataFrame from the list of refined tweets
    df = pd.DataFrame(tweet_list)

    # Save the DataFrame to a CSV file
    df.to_csv('refined_tweets.csv', index=False)

if __name__ == "__main__":
    # Execute the ETL process when the script is run
    run_twitter_etl()
