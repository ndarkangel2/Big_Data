import os

os.environ["SPARK_HOME"] = "D:\\python\\spark-2.3.1-bin-hadoop2.7\\"
os.environ["HADOOP_HOME"]="D:\\python\\winutils"
from pyspark import SparkContext
def to_key_value(x):
    if x == None or len(x) < 1:
        return None
    parts = x.slit(':')
    if len(parts) < 2:
        return ()
    key,value =parts
    value = value.slit('')
    return [(key, value)]

if __name__ == "__main__":
    spark = SparkContext.getOrCreate()
    allNumbers = spark.textFile("D:\\python\\inClassProject\\numbers.txt",1).flatMap(to_key_value)
    allNumbers.