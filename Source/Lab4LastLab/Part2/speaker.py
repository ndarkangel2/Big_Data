# SparkDemo.py
# ﻿This code is copyright (c) 2017 by Laurent Weichberger.
# Authors: Laurent Weichberger, from Hortonworks and,
# from RAND Corp: James Liu, Russell Hanson, Scot Hickey,
# Angel Martinez, Asa Wilks, & Sascha Ishikawa
# This script does use Apache Spark. Enjoy...
# This code was designed to be run as: spark-submit SparkDemo.py
import time
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


# Our filter function:
def filter_tweets(tweet):
    json_tweet = json.loads(tweet)
    if "text" in json_tweet:
        words = json_tweet["text"].split(" ")
        return words
    else:
        return []
#     if 'lang' in json_tweet:  # When the lang key was not present it caused issues
#         if json_tweet['lang'] == 'ar':
#             return True  # filter() requires a Boolean value
#     return False
#
#
# # SparkContext(“local[1]”) would not work with Streaming bc 2 threads are required
sc = SparkContext("local[2]", "Twitter Demo")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 3)  # 10 is the batch interval in seconds
IP = "localhost"
Port = 9999

lines = ssc.socketTextStream(IP, Port)
#
# # When your DStream in Spark receives data, it creates an RDD every batch interval.
# # We use coalesce(1) to be sure that the final filtered RDD has only one partition,
# # so that we have only one resulting part-00000 file in the directory.
# # The method saveAsTextFile() should really be re-named saveInDirectory(),
# # because that is the name of the directory in which the final part-00000 file is saved.
# # We use time.time() to make sure there is always a newly created directory, otherwise
# # it will throw an Exception.
#


# Count each word in each batch
words = lines.flatMap(filter_tweets)
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)


# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.print(num=30)

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate