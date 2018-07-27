import os
os.environ["JAVA_HOME"] = "C:\\Users\\Ndarkangel\\Documents\\449\\Java\\jre1.8.0_171"

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.appName("Lab 4").getOrCreate()
# Load training data

data = spark.read.load(path="Absenteeism_at_work.csv", format="csv", header=True, delimiter=';', schema="""
`ID` INT,
`Reason for absence` INT,
`Month of absence` INT,
`Day of the week` INT,
`Seasons` INT,
`Transportation expense` INT,
`Distance from Residence to Work` INT,
`Service time` INT,
`Age` INT,
`Work load Average/day` FLOAT,
`Hit target` INT,
`Disciplinary failure` INT,
`Education` INT,
`Son` INT,
`Social drinker` INT,
`Social smoker` INT,
`Pet` INT,
`Weight` INT,
`Height` INT,
`Body mass index` INT,
`Absenteeism time in hours` INT
""")

assembler = VectorAssembler(inputCols=[
    # "ID",
    # "Reason for absence",
    "Month of absence",
    "Day of the week",
    # "Seasons",
    "Transportation expense",
    "Distance from Residence to Work",
    "Service time",
    "Age",
    "Work load Average/day",
    "Hit target",
    "Disciplinary failure",
    "Education",
    "Son",
    "Social drinker",
    "Social smoker",
    "Pet",
    "Weight",
    "Height",
    "Body mass index",
    "Absenteeism time in hours"
],outputCol="features")

data = assembler.transform(data)

# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
labelIndexer = StringIndexer(inputCol="Seasons", outputCol="indexedLabel").fit(data)
# Automatically identify categorical features, and index them.
# We specify maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=28).fit(data)

# Split the data into training and test sets (30% held out for testing)
# (trainingData, testData) = data.randomSplit([0.9, 0.1])

# Train a DecisionTree model.
dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

# Chain indexers and tree in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])

# Train model.  This also runs the indexers.
model = pipeline.fit(data)

# Make predictions.
predictions = model.transform(data)

# Select example rows to display.
print("All", predictions.count())
badpredictions = predictions.filter("prediction != indexedLabel")
badpredictions.select("prediction", "indexedLabel", "Seasons", "features", "indexedFeatures").show(20)
print("Bad", badpredictions.count())

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test set accuracy = " + str(accuracy))
print("Error = %g " % (1.0 - accuracy))
treeModel = model.stages[2]
# summary only
print(treeModel)
