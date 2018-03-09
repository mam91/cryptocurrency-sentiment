import pyodbc
import requests
import json
import time
import os
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
import tweepy
from tweepy import OAuthHandler
from textblob import TextBlob
import re

def roundStr(numberToRound):
	return "{:.4f}".format(numberToRound) 
	
def loadConfig(filename):
	config = open(filename)
	data = json.load(config)
	return data

class RedditClient(object):
	#def __init__(self):
	def get_posts(self, endpoint, coinname):
		try:
			subreddit = endpoint.replace("http://www.reddit.com/","")
			hdr = {'User-Agent': 'windows:' + subreddit + '.single.result:v1.0(by /u/mmiller3_ar)'}
			url = endpoint + '.json'
			req = requests.get(url, headers=hdr)
			json_data = json.loads(req.text)
	
			posts = json.dumps(json_data['data']['children'], indent=4, sort_keys=True)
			data_all = json_data['data']['children']
			num_of_posts = 0
		
			while len(data_all) <= 100:
				time.sleep(2)
				last = data_all[-1]['data']['name']
				url = endpoint + '.json?after=' + str(last)
				req = requests.get(url, headers=hdr)
				data = json.loads(req.text)
				data_all += data['data']['children']
				if num_of_posts == len(data_all):
					break
				else:
					num_of_posts = len(data_all)
	
			return data_all
		except exception as e:
			print("Error : " + str(e))
			
class TwitterClient(object):
	def __init__(self):
		# keys and tokens from the Twitter Dev Console
		config = loadConfig('C:\AppCredentials\CoinTrackerPython\\twitter.config')
		
		consumer_key = config[0]["consumer_key"]
		consumer_secret = config[0]["consumer_secret"]
		access_token = config[0]["access_token"]
		access_token_secret = config[0]["access_token_secret"]
		
        # attempt authentication
		try:
			# create OAuthHandler object
			self.auth = OAuthHandler(consumer_key, consumer_secret)
			# set access token and secret
			self.auth.set_access_token(access_token, access_token_secret)
			# create tweepy API object to fetch tweets
			self.api = tweepy.API(self.auth)
		except:
			print("Error: Authentication Failed")
 
	def clean_tweet(self, tweet):
		return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

	def get_tweets(self, query, count = 10):
		tweets = []

		try:
			fetched_tweets = self.api.search(q = query, count = count)

			for tweet in fetched_tweets:
				parsed_tweet = {}

				parsed_tweet['text'] = tweet.text
				#parsed_tweet['sentiment'], parsed_tweet['sentimentString'] = self.get_tweet_sentiment(tweet.text)

				if tweet.retweet_count > 0:
                    # if tweet has retweets, ensure that it is appended only once
					if parsed_tweet not in tweets:
						tweets.append(parsed_tweet)
				else:
					tweets.append(parsed_tweet)
 
			return tweets
 
		except tweepy.TweepError as e:
			print("Error : " + str(e))

class SentimentAnalyzer(object):
	def __init__(self):
		self.sia = SIA()
		self.positive_count = 0
		self.negative_count = 0
		self.neutral_count = 0;
		self.volume = 0
		self.sentimentTotal = 0
		self.positive_threshold = 0.2
		self.negative_threshold = -0.2
		
	def addString(self, stringToAnalyze):
		res = self.sia.polarity_scores(stringToAnalyze)
		self.sentimentTotal = self.sentimentTotal + res['compound']
    
		if res['compound'] > self.positive_threshold:
			self.positive_count = self.positive_count + 1
		elif res['compound'] < self.negative_threshold:
			self.negative_count = self.negative_count + 1
		else:
			self.neutral_count = self.neutral_count + 1
			
		self.volume = self.volume + 1
		
	def reset(self):
		self.positive_count = 0
		self.negative_count = 0
		self.neutral_count = 0;
		self.volume = 0
		self.sentimentTotal = 0
	
	def getVolume(self):
		return str(self.volume)
		
	def getSentimentAverage(self):
		return roundStr(self.sentimentTotal / self.volume)
		
	def getRatios(self):
		posP = self.positive_count / self.volume * 100
		negP = self.negative_count / self.volume * 100
		neuP = self.neutral_count / self.volume * 100
		return roundStr(posP), roundStr(negP), roundStr(neuP)

	
def AnalyzeReddit():
	print("Analyzing Reddit Sentiment", end="", flush=True)

	dbConfig = loadConfig('C:\AppCredentials\CoinTrackerPython\database.config')
	
	con = pyodbc.connect(dbConfig[0]["sql_conn"])
	cursor = con.cursor()
	
	cursor.execute("SELECT t2.url, t1.name FROM [CoinTracker].[dbo].[Market] t1, CoinTracker.dbo.RedditSources t2 where t1.id = t2.coin_fk")
	rows = cursor.fetchall()

	if cursor.rowcount == 0:
		print("No Reddit sources found")
		return;
		
	redditApi = RedditClient()
	analyze = SentimentAnalyzer()
	
	for row in rows:
		print(".", end="", flush=True)
		data_all = redditApi.get_posts(str(row[0]), str(row[1]))

		for post in data_all:
			analyze.addString(post['data']['title'])

		positive_percent, negative_percent, neutral_percent = analyze.getRatios()
	
		params = (str(row[1]), analyze.getSentimentAverage(), analyze.getVolume(), positive_percent, negative_percent, neutral_percent, str(row[0]))
		#print(params)
		cursor.execute("{CALL RefreshMarketSentiment (?,?,?,?,?,?,?)}", params)
		cursor.commit()
		analyze.reset()
	print("Done")
	
	
def AnalyzeTwitter():
	print("Analyzing Twitter Sentiment", end="", flush=True)

	dbConfig = loadConfig('C:\AppCredentials\CoinTrackerPython\database.config')

	con = pyodbc.connect(dbConfig[0]["sql_conn"])
	cursor = con.cursor()
	
	cursor.execute("SELECT name FROM [CoinTracker].[dbo].[Market] where rank < 150 order by rank")
	rows = cursor.fetchall()
	
	if cursor.rowcount == 0:
		print("No records returned")
		return;
		
	api = TwitterClient()
	analyze = SentimentAnalyzer()
		
	for row in rows:
		print(".", end="", flush=True)
		sentimentTotal = 0

		tweets = api.get_tweets(query = str(row[0]), count = 20000)
		
		for i in range(len(tweets)):
			analyze.addString(tweets[i]['text'])

		positive_percent, negative_percent, neutral_percent = analyze.getRatios()
	
		params = (str(row[0]), analyze.getSentimentAverage(), analyze.getVolume(), positive_percent, negative_percent, neutral_percent, str('https://www.twitter.com'))
		#print(params)
		cursor.execute("{CALL RefreshMarketSentiment (?,?,?,?,?,?,?)}", params)
		cursor.commit()
		analyze.reset()
	print("Done")
	
def main():
	AnalyzeReddit()
	AnalyzeTwitter()
	
main()



