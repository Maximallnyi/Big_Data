from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from layers import prepare_dataset
import logging
import mlflow
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator



def run_ml_part(spark):
    df = spark.read.format('delta').load('/app/data/gold_ds')
    
    mlflow.set_tracking_uri("http://0.0.0.0:8118")
    
    if not mlflow.get_experiment_by_name("Price_prediction"):
        mlflow.create_experiment("Price_prediction")
    mlflow.set_experiment("Price_prediction")

    print('Start pipeline training')
    with mlflow.start_run():
        df_train, df_test = df.randomSplit([0.75, 0.25])
        model = LinearRegression(featuresCol="features", labelCol="price")
        model = model.fit(df_train)
        pred_test = model.transform(df_test)

        evaluator = RegressionEvaluator(
            labelCol="price",          
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
        mlflow.spark.log_model(model, "Linear Regression")
        mlflow.log_metric("MSE", mse)
        mlflow.log_metric("MAE", mae)


def main():
    
    spark = SparkSession.builder \
        .appName("SparkApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_3.5:3.2.0") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.3") \

    spark = configure_spark_with_delta_pip(spark).getOrCreate()

    raw_data_path = '/app/data/Clean_Dataset.csv'
    prepare_dataset(spark, raw_data_path)
    run_ml_part(spark)
    spark.stop()


if __name__ == "__main__":
    main()

