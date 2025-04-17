# coding: utf-8
import time
import psutil
import sys
import os
import subprocess

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf

OPTIMIZED = sys.argv[1]

categorical_columns = ['airline', 'flight', 'source_city', 'departure_time', 'stops', 'arrival_time', 'class', 'destination_city']
numeric_columns = ['duration', 'days_left', 'price']


conf = SparkConf()
conf.set("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
conf.set("spark.executor.memory", "1536m")
conf.set("spark.driver.memory", "2g")
conf.set("spark.driver.maxResultSize", "1g")
conf.set("spark.ui.showConsoleProgress", "false")
conf.set("spark.jars.ivy", "/tmp/.ivy2")

spark = SparkSession.builder.appName('test').master("spark://spark-master:7077").config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

start_time = time.time()
process = psutil.Process(os.getpid())

HDFS_PATH = "hdfs:///data/Clean_Dataset.csv"
df = spark.read.csv(HDFS_PATH, header=True, inferSchema=True)

if OPTIMIZED:
    df.cache()
    df = df.repartition(4)

df = df.drop('_c0')

indexers = [
    StringIndexer(inputCol=col, outputCol=col+"_indexed").setHandleInvalid("keep") for col in categorical_columns
]

pipeline = Pipeline(stages=indexers)
df = pipeline.fit(df).transform(df)

vector_to_double_udf = udf(lambda v: float(v[0]), DoubleType())
scaled_df = df
for col_name in numeric_columns:
    if col_name == 'price':
        continue
    assembler = VectorAssembler(inputCols=[col_name], outputCol=col_name+"_vector")
    scaled_df = assembler.transform(scaled_df)
    
    scaler = StandardScaler(inputCol=col_name+"_vector", outputCol=col_name+"_scaled")
    scaler_worker = scaler.fit(scaled_df)
    scaled_df = scaler_worker.transform(scaled_df)
    
    scaled_df = scaled_df.withColumn(col_name+"_scaled", vector_to_double_udf(scaled_df[col_name+"_scaled"]))
    
    scaled_df = scaled_df.drop(col_name+"_vector")
    scaled_df = scaled_df.drop(col_name)

total_df = scaled_df.drop(*(numeric_columns[:-1] + categorical_columns))
assembler = VectorAssembler(
    inputCols= total_df.columns[1:],
    outputCol="features"
)
df = assembler.transform(total_df)

label_indexer = StringIndexer(inputCol="price", outputCol="label")
df = label_indexer.fit(df).transform(df)

df_train, df_test = df.randomSplit([0.75, 0.25])

if OPTIMIZED:
    df_train.cache()
    df_train = df_train.repartition(4)
    df_test.cache()
    df_test = df_test.repartition(4)

model = LinearRegression(featuresCol="features", labelCol="label")
model = model.fit(df_train)
pred_test = model.transform(df_test)

evaluator = RegressionEvaluator(
    labelCol="label",          
    predictionCol="prediction", 
    metricName="mse"            
)

mse = evaluator.evaluate(pred_test)

evaluator = RegressionEvaluator(
    labelCol="label",           
    predictionCol="prediction", 
    metricName="mae"            
)

mae = evaluator.evaluate(pred_test)

if OPTIMIZED:
    df_train.unpersist()
    df_test.unpersist()

total_time = time.time() - start_time
ram_usage = process.memory_info().rss / (1024 * 1024)

print(total_time, ram_usage)

spark.stop()

with open("/log.txt", "a") as f:
    f.write("Time: " + str(total_time) + " seconds, RAM: " + str(ram_usage) + " Mb.\n")