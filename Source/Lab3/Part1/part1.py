import os
os.environ["HADOOP_HOME"]= "C:\\Users\\Ndarkangel\\PycharmProjects\\winutils"

from pyspark import SparkContext

def mapper(theString):
    theString = theString.split(" ")
    user = theString[0]
    friends = theString[1]
    keyvalues = []

    for char in friends:
        keyvalues.append((''.join(sorted(user+char)), friends.replace(char, "")))

    return keyvalues


def reducer(a, b):
    newString = ''
    for char in a:
        if char in b:
            newString += char
    return newString


if __name__ == "__main__":
    mew = SparkContext.getOrCreate()
    lines = mew.textFile("C:\\Users\\Ndarkangel\\PycharmProjects\\Lab3\\Part1\\input", 1)
    newLines = lines.flatMap(mapper)
    newLines.saveAsTextFile("mapper")
    friends = newLines.reduceByKey(reducer)
    friends.coalesce(1).saveAsTextFile("reducer")
    mew.stop()
if __name__ == "__main__":
    facebook = SparkContext.getOrCreate()
    facebooklines = facebook.textFile("C:\\Users\\Ndarkangel\\PycharmProjects\\Lab3\\Part1\\facebook", 1)
    facebookNewLines = facebooklines.flatMap(mapper)
    facebookNewLines.saveAsTextFile("facebookmapper")
    facebookfriends = facebookNewLines.reduceByKey(reducer)
    facebookfriends.coalesce(1).saveAsTextFile("facebookreducer")
    facebook.stop()