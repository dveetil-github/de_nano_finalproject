from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.config("spark.jars.packages",
                                        "saurfang:spark-sas7bdat:2.0.0-s_2.11").enableHiveSupport().getOrCreate()
    df_spark = spark.read.format('com.github.saurfang.sas.spark').load(
        'data_1\\18-83510-I94-Data-2016\\i94_apr16_sub.sas7bdat')
    # df_spark =spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    print(df_spark.first())
