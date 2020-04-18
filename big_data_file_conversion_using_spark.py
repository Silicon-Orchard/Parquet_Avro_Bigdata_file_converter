#!/usr/bin/env python
# coding: utf-8

from os import path
from pyspark.sql import SparkSession

# creating spark session
spark = SparkSession.builder.appName("JSON_PARQUET_AVRO").getOrCreate()


"""
code for json to parquet/avro and vice-versa

"""


"""
Converting JSON file to Parquet

"""
# movies variable read the json file uploaded via website, for this code file in the given path
movies = spark.read.json("example/movielens.json")


my_file_parquet = "movielens.parquet"  # a sample path for parquet file
count = 0
while path.exists(my_file_parquet):  # this code block rename the path if already exists
    # file already exist
    count += 1

    # rename
    my_file_parquet = "{}.{}".format(count, my_file_parquet)


# write/save the the json file into parquet format
movies.write.parquet(my_file_parquet)


"""
Converting Parquet file to JSON

"""

# read the parquet file and bind to parquet variable
parquet = spark.read.parquet(my_file_parquet)

# This view can also be used for data visualization purposes, assuming the file saved in a sql database
parquet.createOrReplaceTempView('parquetlens')
parquet_dataFrame = spark.sql(
    "select * from parquetlens")

# Not necessary to only save file, but can be used for data processing and visualization
print(parquet_dataFrame.show(50))

# sample converted file name for json from parquet
my_json_from_parquet = "movielens_json_from_parquet"
count = 0
while path.exists(my_json_from_parquet):  # same as before, rename the path name
    # file already exist
    count += 1

    # rename
    my_json_from_parquet = "{}.{}".format(count, my_json_from_parquet)

# write/save as json format from parquet
parquet_dataFrame.write.json(my_json_from_parquet)


"""
Converting JSON file to Avro

"""

my_file_avro = "movielens.avro"  # sample file name for avro
count = 0
while path.exists(my_file_avro):  # like as before rename the path
    # file already exist
    count += 1

    # rename
    my_file_avro = "{}.{}".format(count, my_file_avro)

# The spark-avro module is external and not included in spark-submit or spark-shell by default.
movies.write.format("avro").save(my_file_avro)


"""

Converting Avro file to JSON

"""

avro = spark.read.format("avro").load(
    my_file_avro)  # loading avro file into spark

# This view can also be used for data visualization purposes, assuming the file saved in a sql database
avro.createOrReplaceTempView('avrolens')
just_movies = spark.sql(
    "select * from avrolens")

# Not necessary to only save file, but can be used for data processing and visualization
print(just_movies.show(50))


my_json_from_avro = "movielens_json_from_avro"
count = 0
while path.exists(my_json_from_avro):
    # file already exist
    count += 1

    # rename
    my_json_from_avro = "{}.{}".format(count, my_json_from_avro)


# write/save as json format from parquet
just_movies.write.json(my_json_from_avro)

spark.stop()  # closing the spark session


"""
references:
https://spark.apache.org/docs/2.4.0/sql-data-sources-avro.html
https://medium.com/data-science-school/practical-apache-spark-in-10-minutes-part-3-dataframes-and-sql-ac36b26d28e5

"""
