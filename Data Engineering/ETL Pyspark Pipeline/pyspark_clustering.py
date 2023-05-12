import time
from datetime import datetime
import pytz
import pandas as pd
import pandas_gbq
import numpy as np
import urllib.parse
from google.cloud import bigquery
from google.oauth2 import service_account
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.models import DAG
from airflow.sensors.external_task import ExternalTaskSensor
import pyspark
import pyspark.pandas as ps
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import percentile_approx
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from py4j.java_gateway import java_import


credentials = service_account.Credentials.from_service_account_file(r"your_path.json")
project_id = 'latihan-345909'
table_id_src = 'latihan-345909.real_estate.trulia'
table_id_target = 'latihan-345909.real_estate.trulia_clustered'

@task()
def pyspark_clustering():
    spark = SparkSession.builder.appName('pyspark').getOrCreate()
    java_import(spark._sc._jvm, "org.apache.spark.sql.api.python.*")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    
    def spark_preparation(table_id_src, table_name):
        client = bigquery.Client(credentials=credentials, project=project_id)

        sql = """SELECT * FROM {}""".format(table_id_src)

        bq_df = client.query(sql).to_dataframe()
        
        
        #Initiate Spark
        spark = SparkSession.getActiveSession()

        mySchema = StructType([ StructField("id", StringType(), True)\
                        ,StructField("address", StringType(), True)\
                        ,StructField("beds", IntegerType(), True)\
                        ,StructField("baths", IntegerType(), True)\
                        ,StructField("area", IntegerType(), True)\
                        ,StructField("price", IntegerType(), True)\
                        ,StructField("street", StringType(), True)\
                        ,StructField("district", StringType(), True)\
                        ,StructField("city", StringType(), True)\
                        ,StructField("zip_code", StringType(), True)\
                        ,StructField("data_date", DateType(), True)\
                        ,StructField("uid", StringType(), True) ])
        sparkDF=spark.createDataFrame(bq_df, schema=mySchema) 

        
        sparkDF = sparkDF \
            .withColumn("beds", sparkDF["beds"].cast(IntegerType()))   \
            .withColumn("baths", sparkDF["baths"].cast(IntegerType()))   \
            .withColumn("area" , sparkDF["area"].cast(IntegerType()))   \
            .withColumn("price", sparkDF["price"].cast(IntegerType()))
        
        df_clean = sparkDF
        
        #Remove OUTLIER
        
        #Check the Outlier Beds
        q1_beds = df_clean.select(percentile_approx("beds", [0.25], 1000000).alias("quantiles")).collect()[0]['quantiles'][0]
        q3_beds = df_clean.select(percentile_approx("beds", [0.75], 1000000).alias("quantiles")).collect()[0]['quantiles'][0]
        iqr_beds = q3_beds - q1_beds
        top_outlier_beds = q3_beds + 1.5 * iqr_beds
        bottom_outlier_beds= q1_beds - 1.5 * iqr_beds

        
        #Check the Outlier Baths
        q1_baths = df_clean.select(percentile_approx("baths", [0.25], 1000000).alias("quantiles")).collect()[0]['quantiles'][0]
        q3_baths = df_clean.select(percentile_approx("baths", [0.75], 1000000).alias("quantiles")).collect()[0]['quantiles'][0]
        iqr_baths = q3_baths - q1_baths
        top_outlier_baths = q3_baths + 1.5 * iqr_baths
        bottom_outlier_baths = q1_baths - 1.5 * iqr_baths

        #Check the Outlier Area
        q1_area = df_clean.select(percentile_approx("area", [0.25], 1000000).alias("quantiles")).collect()[0]['quantiles'][0]
        q3_area = df_clean.select(percentile_approx("area", [0.75], 1000000).alias("quantiles")).collect()[0]['quantiles'][0]
        iqr_area = q3_area - q1_area
        top_outlier_area = q3_area + 1.5 * iqr_area
        bottom_outlier_area = q1_area - 1.5 * iqr_area
        
        #Check the Outlier Price
        q1_price = df_clean.select(percentile_approx("price", [0.25], 1000000).alias("quantiles")).collect()[0]['quantiles'][0]
        q3_price = df_clean.select(percentile_approx("price", [0.75], 1000000).alias("quantiles")).collect()[0]['quantiles'][0]
        iqr_price = q3_price - q1_price
        top_outlier_price = q3_price + 1.5 * iqr_price
        bottom_outlier_price = q1_price - 1.5 * iqr_price
        
        df_clean = df_clean.filter(df_clean['beds']<=top_outlier_beds)
        df_clean = df_clean.filter(df_clean['baths']<=top_outlier_baths)
        df_clean = df_clean.filter(df_clean['area']<=top_outlier_area)
        df_clean = df_clean.filter(df_clean['price']<=top_outlier_price) 
        
        df = df_clean.dropDuplicates(["beds", "baths", "area", "price", "district", "city"])
        df.createOrReplaceTempView(table_name)
        return "data is saved in {}".format(table_name)


    def spark_clustering(table_name, table_target):
        spark = SparkSession.getActiveSession()
        
        query = "select * from {}".format(table_name)
        df_selected = spark.sql(query)

        #Indexing the string column
        indexer = StringIndexer(inputCols=["city", "district"], 
                            outputCols=["cityIndex", "districtIndex"], 
                            stringOrderType="alphabetAsc")
        indexed = indexer.fit(df_selected).transform(df_selected)

        #OHE the string column
        encoder = OneHotEncoder(inputCols=["cityIndex", "districtIndex"],
                            outputCols=["categoryCity", "categoryDistrict"])
        model = encoder.fit(indexed)
        encoded = model.transform(indexed)

        #Scale integer column
        columns_to_scale = ["area", "price"]
        assemblers = [VectorAssembler(inputCols=[col], outputCol=col + "_vec") for col in columns_to_scale]
        scalers = [MinMaxScaler(inputCol=col + "_vec", outputCol=col + "_scaled") for col in columns_to_scale]
        pipeline = Pipeline(stages=assemblers + scalers)
        scalerModel = pipeline.fit(encoded)
        scaledData = scalerModel.transform(encoded)

        #Make all feature into a vector
        featureassembler=VectorAssembler(inputCols=['beds', 'baths', 'area_scaled', 'price_scaled', 'categoryCity', 'categoryDistrict'],
                                        outputCol="Independent Features")
        feature_output=featureassembler.transform(scaledData)

        #Calculate the best k for kmeans
        silhouette_score=[]
        evaluator = ClusteringEvaluator(predictionCol='prediction', featuresCol='Independent Features', \
                                        metricName='silhouette', distanceMeasure='squaredEuclidean')
        for i in range(2,10):
            KMeans_algo=KMeans(featuresCol='Independent Features', k=i)
            KMeans_fit=KMeans_algo.fit(feature_output)
            output=KMeans_fit.transform(feature_output)
            score=evaluator.evaluate(output)
            silhouette_score.append(score)

        def first_pos(list_score):
            for ind, score in enumerate(list_score):
                if score > 0:
                    return ind

        k_n = first_pos(np.diff(silhouette_score))
        k_neighbor = k_n+2
        k_neighbor

        KMeans_algo=KMeans(featuresCol='Independent Features', k=k_neighbor)
        KMeans_fit=KMeans_algo.fit(feature_output)
        output=KMeans_fit.transform(feature_output)
        score=evaluator.evaluate(output)

        date_now = datetime.now(pytz.timezone("Asia/Jakarta")).date().strftime('%Y-%m-%d')
        kmeans_name = date_now+"_kmeans"
        model_name = date_now+"_kmeans_model"

        kmeans_path = "/home/dana123/airflow/pyspark_model/"+kmeans_name
        KMeans_algo.save(kmeans_path)
        model_path = "/home/dana123/airflow/pyspark_model/"+model_name
        KMeans_fit.save(model_path)

        print("Number of Cluster:", k_neighbor)
        print("Silhouette Score:", score)
        
        output.createOrReplaceTempView(table_target)
        return "data is saved in {}".format(table_target)

    def fetch_data(table_name, table_id_target, project_id):

        spark = SparkSession.getActiveSession()
        client = bigquery.Client(credentials=credentials, project=project_id)
        
        query = "select * from {}".format(table_name)
        output = spark.sql(query)
        
        df_final = output.select("id", "address", "baths", "beds", "area", "price", "street", "district", "city", "zip_code", "data_date", "uid", "prediction").toPandas()
        df_final['id'] = df_final['id'].astype('str')
        df_final['prediction'] = df_final['prediction'].astype('str')
        
        job_config = bigquery.job.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        
        job = client.load_table_from_dataframe(df_final, table_id_target, job_config=job_config)
        job.result()
        spark.stop()

        print("There are {0} rows processed successfully".format(len(df_final)))
    
    data_preparation = spark_preparation(table_id_src, "cleaned_data")
    clustering_data = spark_clustering("cleaned_data", "clustered_data")
    load_data = fetch_data("clustered_data", table_id_target, project_id)

# Declare Dag
with DAG(dag_id='pyspark_clustering',
         schedule_interval="0 0 * * *",
         start_date=datetime(2023, 5, 9),
         catchup=False,
         tags=['etl_gcp'])\
        as dag:
    extract_cluster_load = pyspark_clustering()

    external_task_sensor = ExternalTaskSensor(
    task_id='external_task_sensor',
    poke_interval=60,
    timeout=180,
    soft_fail=False,
    retries=2,
    external_task_id='load',
    external_dag_id='etl_gcp',
    dag=dag)

    external_task_sensor >> extract_cluster_load
