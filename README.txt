
Versions:

Spark : 2.4.0 

Python : 3.7.3

Avro : org.apache.spark:spark-avro_2.11:2.4.0 



How to run:

1. Upload the example directory  and also the source code to your project folder

2.
The spark-avro module is external and not included in spark-submit or spark-shell by default.

As with any Spark applications, spark-submit is used to launch your application. spark-avro_2.11 and its dependencies can be directly added to spark-submit using --packages,
https://spark.apache.org/docs/2.4.0/sql-data-sources-avro.html


Run the command below in your shell directory:

spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.0 big_data_file_conversion_using_spark.py 
