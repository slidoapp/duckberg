import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import bucket


def main(spark):
  spark.sql("DROP TABLE IF EXISTS demo.nyc.taxis")

  df = spark.read.parquet("/home/iceberg/notebooks/notebooks/data/yellow_tripdata_2022-12.parquet")

  logging.info("Inserting data to table ...")
  df = df.sortWithinPartitions("payment_type")
  df.writeTo("demo.nyc.taxis") \
        .using("iceberg") \
        .partitionedBy(bucket(8, "payment_type")) \
        .createOrReplace()
  
  logging.info("Done: Inserting data to table ...")


if __name__ == "__main__":
    main(SparkSession.builder.getOrCreate())