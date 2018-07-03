import os

os.environ["SPARK_HOME"] = "D:\\python\\spark-2.3.1-bin-hadoop2.7\\"
os.environ["HADOOP_HOME"]="D:\\python\\winutils"
from pyspark import SparkContext
if __name__ == "__main__":
    spark = SparkContext.getOrCreate()
    allNumbers = spark.textFile("D:\\python\\inClassProject\\numbers.txt",1)\
        .flatMap(lambda line: line.split(' '))\
        .map(lambda x: (int(x), int(x)))\
        .sortByKey()\
        .keys()
    allNumbers.saveAsTextFile("output")





