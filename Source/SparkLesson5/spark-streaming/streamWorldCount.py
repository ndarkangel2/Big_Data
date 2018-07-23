import sys
import os

os.environ["SPARK_HOME"] = "D:\\python\\spark-2.3.1-bin-hadoop2.7\\"
os.environ["HADOOP_HOME"]="D:\\python\\winutils"

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

"""
This is use for create streaming of text from txt files that creating dynamically 
from files.py code. This spark streaming will execute in each 3 seconds and It'll
show number of words count from each files dynamically
"""


def main():
    sc = SparkContext(appName="PysparkStreaming")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 3)   #Streaming will execute in each 3 seconds
    lines = ssc.socketTextStream("localhost", 1234)
    #lines = ssc.textFileStream('log')  #'log/ mean directory name
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda x: (len(x), [x])) \
        .reduceByKey(lambda a, b: a + b)
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
