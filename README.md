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

If the data set increases by 100X, and we need to convert them to a format which is useful for users or 
data scientists to query and perform analysis, we can save the data in Apache Parquet format and store in Amazon S3.
Amazon S3 will the source of truth for raw data as well as the data ready for analysis. Apache parquet is the columnar storage
designed for large data sets. Amazon S3 provides highly reliable and scalable storage for them.

In order to schedule a job to run on a daily basis, we can implement the Apache Airflow pipeline. It supports the workflow. If any previous steps go in error, the following steps 
will not be executed and can be set up send email alerts so users can check and fix. You can also set up to retry for 2 or 3 times before they marked as failed.

Amazon S3 perform very good for retrieving or uploading thousands of transactions per second per prefix in bucket. 
The read performance can be increased by parallelizing the reads. Amazon S3 gives 5,500 GET/HEAD requests per second per prefix in a bucket.
This can be improved by creating multiple prefixes and parallelize the read and scale the read performance.

#Defending Decisions

Spark is a great tool to read SAS data which is large and have the flexibility to increase nodes when there is need for more data.
The Immigration data model is enriched with more useful information for analysis and can be enriched more with demographcs data easily 
with spark dataframes. Spark sql gives the flexibility to use SQLs for more analysis.

The data scientists want to analyse the US immigration data to find out how many people are coming in various visa types.
 As an example: 
 select count(*), visa_name from immigrationDfTbl group by visa_name;
They want to do the analysis based on ports. The analyst is interested in performing the analysis by which 
ports there are many people entering so they can do better workforce management in those ports.

  

#Quality Checks
there are count checks and added new column with date in correct format.
Performed null checks and filtering for i94visa and i94port in the US immigration data.
Us demographics data is cleaned up with null check for City and state.

#DataModel
Final Immigration data model is as shown below, Data dictionary also provided.

The data model is satisfying the requirements of the identified purpose by the Analyst. The data scientists want to analyse the US immigration data to find out how many people are coming in various visa types.
 As an example: 
 select count(*), visa_name from immigrationDfTbl group by visa_name;
They want to do the analysis based on ports. The analyst is interested in performing the analysis by which 
ports there are many people entering so they can do better workforce management in those ports.

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

#Data Dictionary
 
   year:4 digit year
  month:Numeric month
  city:arrived city
  residence:residence
  us_arrival_dt: landed date in US
  address: address 
  us_departure_dt: departure date from US
  age_yrs: age of repondant in years
  file_added_dt: date added to I-94 files
  visa_issued_dept_state: -->Department of State where where Visa was issued
  us_occupation: Occupation that will be performed in U.S
  arrival_flag: Arrival Flag - admitted or paroled into the U.S.
  departure_flag: Departure Flag - Departed, lost I-94 or is deceased 
  update_flag: Update Flag - Either apprehended, overstayed, adjusted to perm residence
  match_arr_departure__rec_flag: Match flag - Match of arrival and departure records
  year_of_birth: 4 digit year of birth
  us_admitted_dt_to: Date to which admitted to U.S. (allowed to stay until)
  gender: Non-immigrant sex
  ins_number: INS number
  airline_arrived: Airline used to arrive in U.S.
  admission_number:Admission Number 
  flight_number: Flight number of Airline used to arrive in U.S.
  port: port code where entered
  port_name: port name of entry
  visa_type: Class of admission legally admitting the non-immigrant to temporarily stay in U.S.
  visa_name: name of the Class of admission legally admitting the non-immigrant to temporarily stay in U.S.
  model_name: mode used to enter US, land, air or Sea.


#Datasets

SAS data (port, model, visa data )
US demographics data in CSV

