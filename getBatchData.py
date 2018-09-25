import tweepy
from tweepy import OAuthHandler
from accessConfig import *
import json
import codecs
import boto3
import logging
import time
 
auth = OAuthHandler('ZWdr216m2VVgJZxDBmncaeOIV', 'mqSNZFXjXV9cXTNcRhAdTQgIsfnmM0dbQpIFeNmYLzw5QX8RJM')
auth.set_access_token('147905014-5PZyY3fIQSsl6lxqHBGEjptaiixwNkKjLfSLaQqn', 'Elunoc08U824xekcIy3MsOMuudhSmvtU3NhU8BG0CXqMK')
 
api = tweepy.API(auth)
queryHashtag = 'NarendraModi'

def process_or_store(tweet):
    #print(json.dumps(tweet))
    #f = codecs.open('tweetDump.json', 'a','utf-8') #writing to local file.
    try:
        response = firehose_client.put_record(
            DeliveryStreamName='DeepsInputDataStream',
            Record={
                'Data': json.dumps(tweet, ensure_ascii=False, encoding="utf-8")+'\n'
            }
        )
        logging.info(response)
    except Exception:
        logging.exception("Problem pushing to firehose")
    #f.write(json.dumps(tweet, ensure_ascii=False, encoding="utf-8")+'\n')
    #f.close()

firehose_client = boto3.client('firehose', region_name="us-east-1")
LOG_FILENAME = '/tmp/bhargav-twitter-data-stream.log'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)

def main():
   for tweet in tweepy.Cursor(api.search, q=queryHashtag).items(100):
   	process_or_store(tweet._json)

startTime=time.time()
while True:
	if __name__ == "__main__":
	    main()
	time.sleep(1800.0 - time.time() % 60)

