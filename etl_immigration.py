from pyspark.sql import SparkSession
import pandas as pd
from pandas.api.types import is_string_dtype
import datetime as dtime
from datetime import datetime
from pyspark.sql.functions import udf


def readDimDataFiles(data_loc, sep, header, spark):
    """
    read the dim data cleaned up and converted to dataframe
    :return:
    """
    dimDf = pd.read_csv(data_loc, sep=sep, names=header)
    if is_string_dtype(dimDf[header[0]]):
        dimDf[header[0]] = dimDf[header[0]].str.replace("'", "").str.strip()
    if is_string_dtype(dimDf[header[1]]):
        dimDf[header[1]] = dimDf[header[1]].str.replace("'", "").str.strip()
    # print(dimDf.head())
    return spark.createDataFrame(dimDf)


def readUsCitiesDemoData(spark, data_loc, sep=";"):
    """
    read demographics data
    :return:
    """
    demoDf = spark.read.csv(data_loc, sep=sep, header=True, inferSchema=True)
    print("Us Demographics data")
    print(demoDf.show(3))


if __name__ == "__main__":
    spark = SparkSession.builder.config("spark.jars.packages",
                                        "saurfang:spark-sas7bdat:2.0.0-s_2.11").enableHiveSupport().getOrCreate()
    portSpDf = readDimDataFiles("dimension_data/ports.txt", "=", ["port_cd", "port_nm"], spark)
    portSpDf.createOrReplaceTempView("portTable")
    print(portSpDf.show(3))

    modelSpDf = readDimDataFiles("dimension_data/model.txt", "=", ["model_cd", "model_nm"], spark)
    modelSpDf.createOrReplaceTempView("modelTable")
    print(modelSpDf.show(3))

    visaSpDf = readDimDataFiles("dimension_data/visa.txt", "=", ["visa_cd", "visa_nm"], spark)
    visaSpDf.createOrReplaceTempView("visaTable")
    print(visaSpDf.show(3))

    df_spark = spark.read.format('com.github.saurfang.sas.spark').load(
        'data_1\\18-83510-I94-Data-2016\\i94_apr16_sub.sas7bdat')

    # pd.to_datetime( datetime(*xlrd.xldate_as_tuple(x, 0)), format='%Y%m%d')
    # convertDateUdf = udf(lambda x: datetime(*xlrd.xldate_as_tuple(x, 0)).strftime('%Y/%m/%d'))
    convertDateUdf = udf(lambda x: datetime.fromordinal(datetime(1960, 1, 1).toordinal() + int(x)).strftime('%Y/%m/%d'))
    df_spark = df_spark.withColumn("us_arrival_dt", convertDateUdf(df_spark.arrdate))
    df_spark.show(3)

    readUsCitiesDemoData(spark, "data_1/us-cities-demographics.csv", ";")

#     df_spark.createOrReplaceTempView("immigrationTable")
#     immigrationDf = spark.sql("""select
#                                                     immig_dt.i94yr as year,
#                                                     immig_dt.i94mon as month,
#                                                     immig_dt.i94cit as city,
#                                                     immig_dt.i94res as residence,
#                                                     immig_dt.us_arrival_dt as us_arrival_dt,
#                                                     immig_dt.i94addr as address,
#                                                     immig_dt.depdate as us_departure_dt,
#                                                     immig_dt.i94bir as age_yrs,
#                                                     immig_dt.dtadfile as file_added_dt,
#                                                     immig_dt.visapost as visa_issued_dept_state,
#                                                     immig_dt.occup as us_occupation,
#                                                     immig_dt.entdepa as arrival_flag,
#                                                     immig_dt.entdepd as departure_flag,
#                                                     immig_dt.entdepu as update_flag,
#                                                     immig_dt.matflag as match_arr_departure__rec_flag,
#                                                     immig_dt.biryear as year_of_birth,
#                                                     immig_dt.dtaddto as us_admitted_dt_to,
#                                                     immig_dt.gender as gender,
#                                                     immig_dt.insnum as ins_number,
#                                                     immig_dt.airline as airline_arrived,
#                                                     immig_dt.admnum as admission_number,
#                                                     immig_dt.fltno as flight_number,
#                                                     immig_dt.i94port as port,
#                                                     p.port_nm as port_name,
#                                                     immig_dt.visatype as visa_type,
#                                                     visa.visa_nm as visa_name,
#                                                     model.model_nm as model_name
#                                                 from immigrationTable immig_dt left join portTable p on immig_dt.i94port=p.port_cd
#                                                 left join visaTable visa on immig_dt.i94visa = visa.visa_cd
#                                                 left join modelTable model on immig_dt.i94mode = model.model_cd
#                                             """)
# print(immigrationDf.show(3))
# print("Total immigration records in the SAS file--> ")
# print(immigrationDf.count())
#
# immigrationDf.write.parquet("sas_data/immigration_data")
#
# busienssVisaDf = immigrationDf.where(immigrationDf.visa_name == 'Business')
# print("Total immigration records with Business Visa--> ")
# print(busienssVisaDf.count())


