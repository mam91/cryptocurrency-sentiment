import psycopg2
import requests
import json
import os
import sys
import socsentiment.reddit as reddit
import socsentiment.twitter as twitter
import pyprogress.progress as pp

def roundStr(numberToRound):
	return "{:.4f}".format(numberToRound) 
	
def loadConfig(filename):
	config = open(filename)
	data = json.load(config)
	return data

def AnalyzeReddit():
	print("Analyzing Reddit Sentiment")

	dbConfig = loadConfig(r'C:\AppCredentials\CoinTrackerPython\database.config')
	
	con = psycopg2.connect(dbConfig[0]["postgresql_conn"])
	cursor = con.cursor()
	
	cursor.execute("select ss.uri, cc.name, cc.symbol, cc.id, ss.id  from crypto_currencies cc, social_sources ss where cc.id = ss.coin_fk and ss.name = 'reddit' order by cc.id asc")
	
	rows = cursor.fetchall()

	if cursor.rowcount == 0:
		print("No Reddit sources found")
		return
		
	redditApi = reddit.Client('/u/mmiller3')
	
	progress = pp.progress(len(rows))

	for x,row in enumerate(rows):
		try:
			progress.updatePercent(x)
			sentiment = redditApi.analyzeSentiment(str(row[0]))

			params = (row[3], row[4], sentiment.volume, sentiment.sentiment, sentiment.positive, sentiment.negative, sentiment.neutral)
			cursor.callproc('refreshSentiment', params)
		except Exception:
			print("Error analyzing subreddit: " + str(row[0]))
			
	cursor.close()
	con.commit()
	con.close()
	print("Done")
	
	
def AnalyzeTwitter():
	print("Analyzing Twitter Sentiment")

	dbConfig = loadConfig(r'C:\AppCredentials\CoinTrackerPython\database.config')

	con = psycopg2.connect(dbConfig[0]["postgresql_conn"])
	cursor = con.cursor()
	
	cursor.execute("select  cc.id, cc.name, cc.symbol, ss.id from crypto_currencies cc, social_sources ss where ss.name = 'twitter' and cc.id = ss.coin_fk order by cc.id asc limit 150")
	rows = cursor.fetchall()
	
	if cursor.rowcount == 0:
		print("No records returned")
		return

	config = loadConfig(r'C:\AppCredentials\CoinTrackerPython\twitter.config')
		
	consumer_key = config[0]["consumer_key"]
	consumer_secret = config[0]["consumer_secret"]
	access_token = config[0]["access_token"]
	access_token_secret = config[0]["access_token_secret"]
		
	twitterApi = twitter.Client(consumer_key,consumer_secret,access_token,access_token_secret)
	progress = pp.progress(len(rows))

	for x,row in enumerate(rows):
		progress.updatePercent(x)
		sentiment = twitterApi.analyzeSentiment(str(row[0]))

		params = (row[0], row[3], sentiment.volume, sentiment.sentiment, sentiment.positive, sentiment.negative, sentiment.neutral)
		cursor.callproc('refreshSentiment', params)
	cursor.close()
	con.commit()
	con.close()
	print("Done")
	
def main():
	#AnalyzeReddit()
	AnalyzeTwitter()
	
main()



