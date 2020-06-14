#Project Scope

Analyse the US immigration SAS data set. Read the SAS data. Convert the arrival date column to correct format. 
Use the port data to understand and add the port name, Use the visa data to understand the visa type and add to the immigrration data for better understading of the data.
Run count check as part of data quality checks.
Use the final data to know how many records are there with Business Visa.

The port , visa and model data are read and converted to text files with clean up data and proper header. Use this data for 
data analysis.

The final data is saved as parquet format.

Read the US demographics data. 

#Addressing Other Scenarios

The data was increased by 100x. --> Will have to increase the number of nodes in the spark cluster.
The pipelines would be run on a daily basis by 7 am every day. --> Will have to implement the pipeline with Apache Airflow to schedule run on daily basis.
The database needed to be accessed by 100+ people.--> Will have to increase the number of nodes in the spark cluster.

#Defending Decisions

Spark is a great tool to read SAS data which is large and have the flexibility to increase nodes when there is need for more data.
The Immigration data model is enriched with more useful information for analysis and can be enriched more with demographcs data easily 
with spark dataframes. Spark sql gives the flexibility to use SQLs for more analysis.

#Quality Checks
there are count checks and added new column with date in correct format.
Performed null checks and filtering for i94visa and i94port in the US immigration data.
Us demographics data is cleaned up with null check for City and state.

#DataModel
Final Immigration data model

 |-- year: double (nullable = true)
 |-- month: double (nullable = true)
 |-- city: double (nullable = true)
 |-- residence: double (nullable = true)
 |-- us_arrival_dt: string (nullable = true)
 |-- address: string (nullable = true)
 |-- us_departure_dt: double (nullable = true)
 |-- age_yrs: double (nullable = true)
 |-- file_added_dt: string (nullable = true)
 |-- visa_issued_dept_state: string (nullable = true)
 |-- us_occupation: string (nullable = true)
 |-- arrival_flag: string (nullable = true)
 |-- departure_flag: string (nullable = true)
 |-- update_flag: string (nullable = true)
 |-- match_arr_departure__rec_flag: string (nullable = true)
 |-- year_of_birth: double (nullable = true)
 |-- us_admitted_dt_to: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- ins_number: string (nullable = true)
 |-- airline_arrived: string (nullable = true)
 |-- admission_number: double (nullable = true)
 |-- flight_number: string (nullable = true)
 |-- port: string (nullable = true)
 |-- port_name: string (nullable = true)
 |-- visa_type: string (nullable = true)
 |-- visa_name: string (nullable = true)
 |-- model_name: string (nullable = true)

#Datasets

SAS data (port, model, visa data )
US demographics data in CSV

