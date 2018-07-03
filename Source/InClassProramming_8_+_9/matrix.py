import os

os.environ["SPARK_HOME"] = "D:\\python\\spark-2.3.1-bin-hadoop2.7\\"
os.environ["HADOOP_HOME"]= "D:\\python\\winutils"
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    lines = sc.textFile("D:\\python\\inClassProject\\matrix.txt",1).collect()

    keyvalues = []
    additionplace = ""
    finalrow, finalcol = lines[0].split(" ")
    lines = lines[1:]

    # FIRST MAP
    for line in lines:
        ourmatrix, ourrow, ourcol, value = line.split(" ")
        if int(ourmatrix) == 1:
            additionplace = ourrow
        else:
            additionplace = ourcol

        if int(ourmatrix) == 1:
            for i in range(1, int(finalrow) + 1):
                keyvalues.append(((str(i) + "," + ourcol + "," + additionplace), int(value)))
        else:
            for i in range(1, int(finalcol) + 1):
                keyvalues.append(((ourrow + "," + str(i) + "," + additionplace), int(value)))

    #FIRST REDUCE
    counts = sc.parallelize(keyvalues)
    counts = counts.reduceByKey(mul)

    #SECOND MAP
    matrixtwo = counts.map(lambda x : (x[0][0:3], x[1])).collect()

    #SECOND REDUCE
    counts = sc.parallelize(matrixtwo).reduceByKey(add)

    counts.coalesce(1).saveAsTextFile("output3")
    sc.stop()