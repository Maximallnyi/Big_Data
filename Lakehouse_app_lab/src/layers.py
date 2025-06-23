import logging

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, FloatType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder

from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf

categorical_columns = ['airline', 'flight', 'source_city', 'departure_time', 'stops', 'arrival_time', 'class', 'destination_city']
numeric_columns = ['duration', 'days_left', 'price']



def prepare_df(df):

    all_columns = ['airline', 'flight', 'source_city', 'departure_time', 'stops', 'arrival_time', 'class', 'destination_city', 'duration', 'days_left', 'price']

    numeric_dict = {'float':['duration'], 'integer': ['days_left', 'price'] }
    for type in numeric_dict:
        for column in numeric_dict[type]:
            if type == 'integer':
                df = df.withColumn(column, df[column].cast(IntegerType()))
            if type == 'float':
                df = df.withColumn(column, df[column].cast(FloatType()))

    df = df.select(*all_columns)

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

    return df


def prepare_dataset(spark, input_path):

    #bronze
    raw_data = spark.read.csv(input_path, header=True)
    raw_data.repartition(spark.sparkContext.defaultParallelism) \
        .write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .save("/app/data/bronze_ds")
    raw_data = raw_data.drop('_c0')

    #silver
    data = (spark.read.format('delta').load('/app/data/bronze_ds')
            .repartition(spark.sparkContext.defaultParallelism)
            .dropna(subset=['days_left', 'destination_city', 'flight'])
            )
    data.repartition(spark.sparkContext.defaultParallelism) \
        .write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .save("/app/data/silver_ds")
    
    #gold
    data = spark.read.format('delta').load('/app/data/silver_ds')
    data = prepare_df(data)
    data.repartition(spark.sparkContext.defaultParallelism) \
        .write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .save("/app/data/gold_ds")
    