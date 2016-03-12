#!/usr/bin/env python

from __future__ import print_function

import sys
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def get_people_and_hashtags(tweet):
    data = json.loads(tweet)
    try:
        author = "@" + data['user']['screen_name']
        mentions = [user["screen_name"] for user in data['entities']['user_mentions']]
        people = mentions + [author]
        hashtags = ["#" + hashtag["text"] for hashtag in data['entities']['hashtags']]
        return (tuple(people), tuple(hashtags))
    except KeyError:
        return ()


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

    # Tweet processing
    lines = tweets.map(lambda x: get_people_and_hashtags(x[1])).filter(lambda x: len(x)>0)
    lines.persist()
    lines.pprint()

    ssc.start()
    ssc.awaitTermination()

