#!/usr/bin/env python


# import required libraries
from kafka import SimpleProducer, KafkaClient
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from twitter_config import consumer_key, consumer_secret, access_token, access_token_secret

# Kafka settings
topic = b'twitter-stream'
# setting up Kafka producer
kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)


#This is a basic listener that just put received tweets to kafka cluster.
class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages(topic, data.encode('utf-8'))
        #print data
        return True

    def on_error(self, status):
        print status

WORDS_TO_TRACK = "the to and is in it you of for on my that at with me do have just this be so are not was but out up what now new from your like good no get all about we if time as day will one how can some an am by going they go or has know today there love more work too got he back think did when see really had great off would need here thanks been still people who night want why home should well much then right make last over way does getting watching its only her post his morning very she them could first than better after tonight our again down news man looking us tomorrow best into any hope week nice show yes where take check come fun say next watch never bad free life".split()

if __name__ == '__main__':
    print 'running the twitter-stream python code'
    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    # Goal is to keep this process always going
    while True:
        try:
           # stream.sample()
            stream.filter(languages=["en"], track=WORDS_TO_TRACK)
        except:
            pass
