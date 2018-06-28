import os

os.environ["SPARK_HOME"] = "D:\\python\\spark-2.3.1-bin-hadoop2.7\\"
os.environ["HADOOP_HOME"]="D:\\python\\winutils"
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
    spark = SparkContext.getOrCreate()
    lines = spark.textFile("D:\\python\\inClassProject\\sample.txt",1)
    allCharacters = lines.flatMap(lambda x: list(x))
    vowels = allCharacters.filter(lambda c: c in ['a','e','i','o','u'])
    b = vowels.map(lambda x:('vowels',1))
    counts = b.reduceByKey(add)

    print(counts)
    counts.saveAsTextFile("output")

    spark.stop()