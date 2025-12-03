# Databricks notebook source
spark

# COMMAND ----------


df=spark.read.format("csv")\
  .option("header","true")\
  .option("inferSchema","false")\
  .option("mode","FAILFAST")\
  .load("/Workspace/Users/divyakuruba3@gmail.com/my-prep/my_prep_spark/flight-data.csv")

# COMMAND ----------

df.show(5)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df_schema_changes=spark.read.format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
  .option("mode","FAILFAST")\
  .load("/Workspace/Users/divyakuruba3@gmail.com/my-prep/my_prep_spark/flight-data.csv")

# COMMAND ----------

df_schema_changes.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

spark


# COMMAND ----------

my_schema=StructType([
    StructField("name_1",StringType(),True),
    StructField("name_2",StringType(),True),
    StructField("count",IntegerType(),True)
])


# COMMAND ----------

flight_data_2=spark.read.format("csv")\
    .option("header","false")\
    .option("infershema","false")\
    .schema(my_schema)\
    .load("/Workspace/Users/divyakuruba3@gmail.com/my-prep/my_prep_spark/flight-data.csv")

flight_data_2.show()  

# COMMAND ----------

flight_data_3=spark.read.format("csv")\
    .option("header","false")\
    .option("infershema","false")\
    .option("skipRows","1")\
    .schema(my_schema)\
    .load("/Workspace/Users/divyakuruba3@gmail.com/my-prep/my_prep_spark/flight-data.csv")

flight_data_3.show() 

# COMMAND ----------

spark

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

emp_schema=StructType([
    StructField("id",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("age",StringType(),True),
    StructField("salary",IntegerType(),True),
    StructField("address",StringType(),True),
    StructField("nominee",StringType(),True)
])

# COMMAND ----------

emp_data=spark.read.format("csv")\
                 .option("header","false")\
                 .option("SkipRows",1)\
                 .option("infershema","false")\
                 .schema(emp_schema)\
                 .option("mode","PREMISSIVE")\
                .load("/Workspace/Users/divyakuruba3@gmail.com/my-prep/my_prep_spark/employee.csv")

# COMMAND ----------


#corrupted data : premmisive
emp_data.show()

# COMMAND ----------

#FAILFAST
emp_data_failfast=spark.read.format("csv")\
                 .option("header","false")\
                 .option("SkipRows",1)\
                 .option("infershema","false")\
                 .schema(emp_schema)\
                 .option("mode","FAILFAST")\
                .load("/Workspace/Users/divyakuruba3@gmail.com/my-prep/my_prep_spark/employee.csv")

# COMMAND ----------

emp_data_failfast.show()

# COMMAND ----------

#dropmalformed
emp_data_dropmalformed=spark.read.format("csv")\
                 .option("header","false")\
                 .option("SkipRows",1)\
                 .option("infershema","false")\
                 .schema(emp_schema)\
                 .option("mode","DROPMALFORMED")\
                .load("/Workspace/Users/divyakuruba3@gmail.com/my-prep/my_prep_spark/employee.csv")

emp_data_dropmalformed.show()


# COMMAND ----------

#how can print bad records?

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

emp_schema_with_bad_records_schema_1=StructType([
    StructField("id",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("age",StringType(),True),
    StructField("salary",IntegerType(),True),
    StructField("address",StringType(),True),
    StructField("nominee",StringType(),True),
    StructField("bad_records",StringType(),True)
])

emp_data_with_badrecords_1=spark.read.format("csv")\
                 .option("header","true")\
                 .option("infershema","true")\
                 .option("mode","PERMISSIVE")\
                 .option("columnNameOfCorruptRecord","bad_records")\
                 .schema(emp_schema_with_bad_records_schema_1)\
                 .load("/Workspace/Users/divyakuruba3@gmail.com/my-prep/my_prep_spark/employee.csv")

emp_data_with_badrecords_1.show(truncate=False)

# COMMAND ----------

#how can print bad records?

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

emp_schema_with_bad_records_schema_1=StructType([
    StructField("id",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("age",StringType(),True),
    StructField("salary",IntegerType(),True),
    StructField("address",StringType(),True),
    StructField("nominee",StringType(),True),
    StructField("bad_records",StringType(),True)
])

emp_data_with_badrecords_1=spark.read.format("csv")\
                 .option("header","true")\
                 .option("infershema","true")\
                 .option("columnNameOfCorruptRecord","bad_records")\
                 .schema(emp_schema_with_bad_records_schema_1)\
                 .option("badRecordsPath","/Workspace/Users/divyakuruba3@gmail.com/my-prep/my_prep_spark/bad_records")\
                 .load("/Workspace/Users/divyakuruba3@gmail.com/my-prep/my_prep_spark/employee.csv")

emp_data_with_badrecords_1.show(truncate=False)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Workspace/Users/divyakuruba3@gmail.com/my-prep/my_prep_spark/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Workspace/Users/divyakuruba3@gmail.com/my-prep/my_prep_spark/

# COMMAND ----------

spark

data_flight=spark.read.format("csv")\
						.option("inferschema","true")\
						.option("header","true")\
						.option("mode","PERMISSIVE")\
						.load("/Workspace/Users/divyakuruba3@gmail.com/my-prep/my_prep_spark/flight-data.csv")

# COMMAND ----------

data_flight.show(10)

# COMMAND ----------

#filer
#DEST_COUNTRY_NAME,ORIGIN_COUNTRY_NAME,count
from pyspark.sql.functions import col

filter_data1=data_flight.filter(col("DEST_COUNTRY_NAME")=="United States")
filter_data1.show()

# COMMAND ----------

#repartition

repartition_dara=data_flight.repartition(3)

repartition_dara.show()

# COMMAND ----------

#group by (DEST_COUNTRY_NAME,count(ORIGIN_COUNTRY_NAME) group by DEST_COUNTRY_NAME)

groupby_data=data_flight.groupby("DEST_COUNTRY_NAME").count().filter("count > 1")
groupby_data.show()

# COMMAND ----------

df_json_data=spark.read.format("json")\
						.option("inferSchema","true")\
						.option("mode","PERMISSIVE")\
						.load("/Workspace/Users/divyakuruba3@gmail.com/my-prep/my_prep_spark/line_delimited_json.json").show()


# COMMAND ----------

df_json_data_extra_Key=spark.read.format("json")\
						.option("inferSchema","true")\
						.option("mode","PERMISSIVE")\
						.load("/Workspace/Users/divyakuruba3@gmail.com/my-prep/my_prep_spark/single_file_json.json").show()


# COMMAND ----------

df_json_data_extra_Key=spark.read.format("json")\
						.option("inferSchema","true")\
						.option("mode","PERMISSIVE")\
                        .option("multiLine","true")\
						.load("/Workspace/Users/divyakuruba3@gmail.com/my-prep/my_prep_spark/Multi_line_correct.json").show()


# COMMAND ----------

df_json_data_extra_Key=spark.read.format("json")\
						.option("inferSchema","true")\
						.option("mode","PERMISSIVE")\
                        .option("multiLine","true")\
						.load("/Workspace/Users/divyakuruba3@gmail.com/my-prep/my_prep_spark/Multi_line_incorrect.json").show()

