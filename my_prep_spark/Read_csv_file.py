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
