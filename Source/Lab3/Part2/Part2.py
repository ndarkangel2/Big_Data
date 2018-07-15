import os


os.environ["HADOOP_HOME"]="C:\\Users\\Ndarkangel\\PycharmProjects\\winutils"
from operator import add
from pyspark.sql import *

spark = SparkSession.builder \
    .appName("Lab3") \
    .getOrCreate()

df = spark.read.load(path="WorldCupMatches.csv",
                     format='csv',
                     schema="""
Year INT,
Datetime STRING,
Stage STRING,
Stadium STRING,
City STRING,
`Home Team Name` STRING,
`Home Team Goals` INT,
`Away Team Goals`	INT,
`Away Team Name` STRING,
`Win conditions` STRING,
Attendance INT,
`Half-time Home Goals` INT,
`Half-time Away Goals` INT,
Referee	 STRING,
`Assistant 1` STRING,
`Assistant 2` STRING,
RoundID	 STRING,
MatchID	 STRING,
`Home Team Initials` STRING,
`Away Team Initials` STRING
""",
                     option=('header', 'true'))
#f.show()
df.registerTempTable("WorldCupMatches")
sqlContext = SQLContext(spark)

matches = df.rdd.filter(lambda x: x['Year'] is not None)














# SQL

# 1. which year had highest scoring games (on average)

#sqlContext.sql("SELECT year, MAX(`Home Team Goals`) HomeGoals, MAX(`Away Team Goals`) AwayGoals FROM WorldCupMatches WHERE year is not NULL GROUP BY year").show()

# 2. average attendance

#sqlContext.sql("SELECT AVG(Attendance) FROM WorldCupMatches").show()

# 3. most common referee for each year

#sqlContext.sql(
#"SELECT year, referee, COUNT(Referee) as CNT FROM WorldCupMatches WHERE year is not NULL GROUP BY Referee, year ORDER By CNT DESC").show()

# 4. list of all referees

#sqlContext.sql("SELECT DISTINCT (referee) FROM WorldCupMatches").show()

# 5. number of games descending by year

#sqlContext.sql(
  #  "SELECT year, COUNT(1) as Games FROM WorldCupMatches WHERE year is not NULL GROUP BY year ORDER By Year DESC").show()

# MAPREDUCE


# 1. which city has hosted the most games
#SQL
#sqlContext.sql("SELECT  City, COUNT(City) FROM WorldCupMatches Group by City").show()

#SDF
#df.groupBy("City").count().show()

#RDD
citiesMapped = matches.map(lambda x: (x['City'], 1))
countCities = citiesMapped.reduceByKey(add)
#countCities.saveAsTextFile('countCities')
# print(countCities.collect())



# 2. who was in group one each year
#SQL
#sqlContext.sql("SELECT Year, `Home Team Name` AS Name FROM WorldCupMatches WHERE Stage = 'Group 1' UNION SELECT Year, `Away Team Name` AS Name FROM WorldCupMatches WHERE Stage = 'Group 1' ORDER By YEAR").show()

#RDF

#homeGroup1Df = df.select(df["Year"], df['Home Team Name'], df['Stage']).where(df['Stage'] == 'Group 1').withColumnRenamed("Home Team Name", "Name")
#awayGroup1Df = df.select(df["Year"], df['Away Team Name'], df['Stage']).where(df['Stage'] == 'Group 1').withColumnRenamed("Away Team Name", "Name")
#group1Df = homeGroup1Df.union(awayGroup1Df).drop('Stage').distinct()
#group1Df.show()


#RDD
def concatnames(a, b):
    return a + ', ' + b

group1 = matches.filter(lambda match: match['Stage'] == 'Group 1')
homeByYear = group1.map(lambda x: (x['Year'], x['Home Team Name']))
awayByYear = group1.map(lambda x: (x['Year'], x['Away Team Name']))
teamsByYear = homeByYear.union(awayByYear).distinct()
group1Teams = teamsByYear.reduceByKey(concatnames)
#group1Teams.saveAsTextFile('group1Teams')
# print(group1Teams.collect())


# 3. total attendance per year
#SQL
#sqlContext.sql("SELECT Year,  SUM(Attendance)  FROM WorldCupMatches group by Year").show()

#RDF
#df.groupBy("Year").sum("Attendance").show()

#RDD
filterAttendance = matches.filter(lambda x: x['Attendance'] is not None)
mapAttendance = filterAttendance.map(lambda x: (x['Year'], int(x['Attendance'])))
countAttendance = mapAttendance.reduceByKey(add)
#countAttendance.saveAsTextFile('countAttendance')
# print(countAttendance.collect())



# 4. which games had all goals in the second half
#SQL
#sqlContext.sql("SELECT MatchID, (`Home Team Goals` + `Away Team Goals`) total  FROM WorldCupMatches WHERE `Half-time Home Goals` = 0 AND `Half-time Away Goals` = 0").show()

#RDF
#df.select(df['Home Team Goals']+ df['Home Team Goals']).show()
#df.select(df["MatchID"], df['Half-time Home Goals'], df['Half-time Away Goals'], df['Home Team Goals'] + df['Away Team Goals']).where(df['Half-time Home Goals'] == 0).where(df['Half-time Away Goals'] == 0).show()

#RDD
def nineMapper(row):
    firsthalf = row['Half-time Home Goals'] + row['Half-time Away Goals']
    if firsthalf == 0:
        secondhalf = row['Home Team Goals'] + row['Away Team Goals']
        return [(row['MatchID'], secondhalf)]
    else:
        return []


lastHalfGoals = matches.flatMap(nineMapper).distinct()
#lastHalfGoals.saveAsTextFile('lastHalfGoals')

#print(lastHalfGoals.collect())


# 5. Percent of away games by team
#SQL
#sqlContext.sql("""SELECT Home.Name AS NAME, CONCAT(Int(100 * AwayGames / (AwayGames + HomeGames)), '%') AS PercentAway FROM (SELECT `Home Team Name` AS Name, COUNT(*) as HomeGames FROM WorldCupMatches GROUP BY Name HAVING Name IS NOT NULL) AS Home JOIN (SELECT `Away Team Name` AS Name, COUNT(*) as AwayGames FROM WorldCupMatches GROUP BY Name HAVING Name IS NOT NULL) AS Away ON Home.Name = Away.Name""").show()

#RDF
#homeDf = df.groupBy("Home Team Name").count().withColumnRenamed("count", "HomeGames").withColumnRenamed("Home Team Name", "Name")
#awayDf = df.groupBy("Away Team Name").count().withColumnRenamed("count", "AwayGames").withColumnRenamed("Away Team Name", "Name")
#joinedDf = homeDf.join(awayDf, ["Name"])
#joinedDf.withColumn('PercentAway', (100 * joinedDf["AwayGames"] / (joinedDf["AwayGames"] + joinedDf["HomeGames"]))).show()

#RDD
def percent(a, b):
    return str(int(a / (a + b) * 100)) + '%'

home = matches.map(lambda x: (x['Home Team Name'], 1))
away = matches.map(lambda x: (x['Away Team Name'], 1))
homeCounts = home.reduceByKey(add)
# print(homeCounts.collect())
awayCounts = away.reduceByKey(add)
# print(awayCounts.collect())
teams = awayCounts.union(homeCounts)
percentAway = teams.reduceByKey(percent)
#percentAway.saveAsTextFile('percentAway')
# print(percentAway.collect())
