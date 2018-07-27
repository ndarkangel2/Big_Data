import os
os.environ["JAVA_HOME"] = "C:\\Users\\Ndarkangel\\Documents\\449\\Java\\jre1.8.0_171"

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Load and parse the data file, converting it to a DataFrame.
spark = SparkSession.builder.appName("Lab 4").getOrCreate()

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
# Set maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])

# Train a RandomForest model.
rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=10)

# Convert indexed labels back to original labels.
labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                               labels=labelIndexer.labels)

# Chain indexers and forest in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf, labelConverter])

# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("predictedLabel", "Seasons", "features").show(50)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test set accuracy = " + str(accuracy))
print("Test Error = %g" % (1.0 - accuracy))

treeModel = model.stages[2]
# summary only
print(treeModel)