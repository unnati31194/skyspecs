# Databricks notebook source
# DBTITLE 1,Import Libraries
import sklearn
from sklearn.datasets import fetch_california_housing
import pandas as pd
from pyspark.sql.types import StructType, DoubleType, StructField

# COMMAND ----------

# DBTITLE 1,Fetch Dataset
data = fetch_california_housing(as_frame= True)
df_housing = pd.DataFrame(data.data, columns=data.feature_names)

# COMMAND ----------

# DBTITLE 1,Define Schema of the dataset
mySchema = StructType([ 
    StructField("MedInc", DoubleType(), True) \
    ,StructField("HouseAge", DoubleType(), True) \
    ,StructField("AveRooms", DoubleType(), True) \
    ,StructField("AveBedrms", DoubleType(), True) \
    ,StructField("Population", DoubleType(), True) \
    ,StructField("AveOccup", DoubleType(), True) \
    ,StructField("Latitude", DoubleType(), True) \
    ,StructField("Longitude", DoubleType(), True) \
])

# COMMAND ----------

# DBTITLE 1,Write table in delta format
sparkDF = spark.createDataFrame(df_housing, schema=mySchema)
output_path = "/FileStore/tables/unnati/"
sparkDF.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(output_path)

# COMMAND ----------

# DBTITLE 1,Display data
sparkDF = spark.read.format("delta").load(output_path)
display(sparkDF)

# COMMAND ----------

# DBTITLE 1,Create database and delta table
spark.sql("create database if not exists practise")

spark.sql("use practise")

spark.sql( """CREATE TABLE if not exists practise.housing_dataset
                   USING DELTA
                   LOCATION '/FileStore/tables/unnati/'
                   """)


# COMMAND ----------

# DBTITLE 1,Functions to be used in unit tests
# Does the specified table exist in the specified database?
def tableExists(tableName, dbName):
    return spark.catalog.tableExists(f"{dbName}.{tableName}")

# Does the specified column exist in the given DataFrame?
def columnExists(dataFrame, columnName):
    if columnName in dataFrame.columns:
        return True
    else:
        return False

# Has data moved in the sink table?
def numOfRowsInTheData(dataFrame):
    return df.count()

# COMMAND ----------

# DBTITLE 1,Call functions defined above
tableName = "housing_dataset"
dbName = "practise"
columnList = sparkDF.columns

if tableExists(tableName, dbName):

    df = spark.sql(f"SELECT * FROM {dbName}.{tableName}")

    # And the specified column exists in that table...
    for columnName in columnList:
        if columnExists(df, columnName):
            print(f"Column '{columnName}' exist in table '{tableName}' in schema (database) '{dbName}'.")
        else:
            print(f"Column '{columnName}' does not exist in table '{tableName}' in schema (database) '{dbName}'.")
   
    # If data exists in that table...
    if numOfRowsInTheData(df) > 0:
        numRows= numOfRowsInTheData(df)
        print(f"There are {numRows} rows in '{tableName}'.")
    else:
        print(f" No data present in '{tableName}'.")
        
else:
    print(f"Table '{tableName}' does not exist in schema (database) '{dbName}'.") 

# COMMAND ----------


