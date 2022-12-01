import os
import datetime
import pandas as pd
from pyarrow.parquet import ParquetDataset
from pyspark.sql import SparkSession

PATH_TO_CHANGE = '/Users/angel/airflow'


def formattingDataJSON(**kwargs):
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    path = PATH_TO_CHANGE + '/DataLake/raw/source2/DataEntity1/' + str(datetime.date.today()) + '/moviesAPI.json'
    df = spark.read.json(path)

    pathParquetJSON = PATH_TO_CHANGE + "/DataLake/formatted/source2/DataEntity1/" + str(
        datetime.date.today()) + "/moviesAPI.parquet"

    if not os.path.exists(pathParquetJSON):
        df.write.parquet(pathParquetJSON)

    print("Data formated : done")


def formattingDataCSV(**kwargs):
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    path = PATH_TO_CHANGE + '/DataLake/raw/source1/DataEntity1/2022-05-29/movies.csv'

    df2 = spark.read.option("header", True).csv(path)
    df2.printSchema()

    pathDate = PATH_TO_CHANGE + '/DataLake/formatted/source1/DataEntity1/' + str(datetime.date.today())
    if not os.path.exists(pathDate):
        os.makedirs(pathDate)

    pathParquetCSV = PATH_TO_CHANGE + "/DataLake/formatted/source1/DataEntity1/" + str(
        datetime.date.today()) + "/movies.parquet"

    if not os.path.exists(pathParquetCSV):
        df2.write.parquet(pathParquetCSV)

    print("Data formated : done")


def combineData(**kwargs):
    pathParquetJSON = PATH_TO_CHANGE + "/DataLake/formatted/source2/DataEntity1/" + str(
        datetime.date.today()) + "/moviesAPI.parquet"

    pathParquetCSV = PATH_TO_CHANGE + "/DataLake/formatted/source1/DataEntity1/" + str(
        datetime.date.today()) + "/movies.parquet"

    datasetJSON = ParquetDataset(pathParquetJSON)
    tableJSON = datasetJSON.read()
    df = tableJSON.to_pandas()

    datasetCSV = ParquetDataset(pathParquetCSV)
    tableCSV = datasetCSV.read()
    df2 = tableCSV.to_pandas()
    del df2['id']
    del df2['_c0']

    dfF = pd.merge(df, df2, on='title')

    dfF.popularity = dfF.popularity.astype(float)
    dfF.vote_average = dfF.vote_average.astype(float)
    dfF.vote_count = dfF.vote_count.astype(int)
    dfF.release_date = pd.to_datetime(dfF.release_date)

    print(dfF.dtypes)

    pathDate = PATH_TO_CHANGE + '/DataLake/usage/my_usage1/DataEntity1/' + str(datetime.date.today())
    if not os.path.exists(pathDate):
        os.makedirs(pathDate)

    pathParquetFinal = PATH_TO_CHANGE + "/DataLake/usage/my_usage1/DataEntity1/" + str(
        datetime.date.today()) + "/moviesData.parquet"

    dfF.to_parquet(pathParquetFinal)
    moviesParquet = pd.read_parquet(pathParquetFinal, engine='pyarrow')

    pathCSVDate = PATH_TO_CHANGE + "/DataLake/usage/my_usage2/DataEntity1/" + str(
        datetime.date.today())
    if not os.path.exists(pathCSVDate):
        os.makedirs(pathCSVDate)

    pathCSVFinal = PATH_TO_CHANGE + "/DataLake/usage/my_usage2/DataEntity1/" + str(
        datetime.date.today()) + "/moviesData.csv"

    moviesParquet.to_csv(pathCSVFinal, index=False)

    print("Data combined : done")
