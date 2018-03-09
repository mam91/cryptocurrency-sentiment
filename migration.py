import psycopg2
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

def AnalyzeReddit():
	dbConfig = loadConfig('C:\AppCredentials\CoinTrackerPython\database.config')
	
	con = pyodbc.connect(dbConfig[0]["sql_conn"])
	cursor = con.cursor()
	
	conPg = psycopg2.connect(dbConfig[0]["postgresql_conn"])
	cursorPg = conPg.cursor()
	
	cursor.execute("SELECT t1.name, t2.source, t2.url FROM [CoinTracker].[dbo].[Market] t1, CoinTracker.dbo.RedditSources t2 where t1.id = t2.coin_fk")
	rows = cursor.fetchall()

	if cursor.rowcount == 0:
		print("No Reddit sources found")
		return;
		
	
	for row in rows:
		print(".", end="", flush=True)
		params = (row[0], row[1], row[2])
		cursorPg.callproc('insertRedditSource', params)
	cursorPg.close()
	conPg.commit()
	conPg.close()
	
	cursor.close()
	con.close()
	print("Done")
	
def main():
	AnalyzeReddit()
	
main()



