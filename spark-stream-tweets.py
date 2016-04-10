#!/usr/bin/env python

from __future__ import print_function

import sys
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def get_people_with_hashtags(tweet):
    """
    Returns (people, hashtags) if successful, otherwise returns empty tuple. All users
    except author have an @ sign appended to the front.
    """
    data = json.loads(tweet)
    try:
        hashtags = ["#" + hashtag["text"] for hashtag in data['entities']['hashtags']]
        # Tweets without hashtags are a waste of time
        if len(hashtags) == 0:
            return ()
        author = data['user']['screen_name']
        mentions = ["@" + user["screen_name"] for user in data['entities']['user_mentions']]
        people = mentions + [author]
        return (people, hashtags)
    except KeyError:
        return ()

def filter_out_unicode(x):
    """
    Pass in a list of (authors, hashtags) and return a list of hashtags that are not unicode
    """
    result = []
    for hashtag in x[1]:
        try:
            result.append(str(hashtag))
        except UnicodeEncodeError:
            pass
    return result

if __name__ == "__main__":
    zkQuorum = "localhost:2181"
    topic = "twitter-stream"

    # User-supplied command arguments
    if len(sys.argv) != 3:
        print("Usage: spark-stream-tweets.py <n_top_hashtags> <seconds_to_run>")
        exit(-1)
    n_top_hashtags = int(sys.argv[1])
    seconds_to_run = int(sys.argv[2])

    sc = SparkContext("local[2]", appName="TwitterStreamKafka")
    ssc = StreamingContext(sc, seconds_to_run)

    tweets = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    # Tweet processing. 
    # Kafka passes a tuple of message ID and message text. Message text is the tweet text.
    # All tweets are turned into ([people],[hashtags]) and tweets without hashtags are filtered
    # out.
    lines = tweets.map(lambda x: get_people_with_hashtags(x[1])).filter(lambda x: len(x)>0)
    lines.cache()
    hashtags = lines.flatMap(filter_out_unicode).map(lambda i: (i, 1)).reduceByKey(lambda x,y: x+y)
    just_hashtags = hashtags.map(lambda (k,v): (v,k))
    top_hashtags = just_hashtags.filter(lambda x: x[0] > 2)
# This line causes exponential processing increases
#    sorted_hashtags = just_hashtags.transform(lambda x: x.sortByKey(False))
#    top_hashtags.pprint()

    # Get authors
   # lines.pprint()


    ssc.start()
    ssc.awaitTermination()

