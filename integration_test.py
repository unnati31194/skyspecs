# Databricks notebook source
# DBTITLE 1,Install Libraries
import sklearn
from sklearn.datasets import fetch_california_housing
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Fetch source dataset
data = fetch_california_housing(as_frame= True)
df_housing = pd.DataFrame(data.data, columns=data.feature_names)

# COMMAND ----------

# DBTITLE 1,Write into Delta table
source_DF = spark.createDataFrame(df_housing)
output_path = "/FileStore/tables/unnati_test/"
source_DF.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(output_path)

# COMMAND ----------

# DBTITLE 1,Read sink dataset
sink_DF = spark.read.format("delta").load(output_path)

# COMMAND ----------

# DBTITLE 1,Function to check whether all rows from source got copied into sink dataset
def matchRows (source_dataset, sink_dataset):
    countRowsSource = source_dataset.count()
    countRowsSink = sink_dataset.count()
    return countRowsSource == countRowsSink

# COMMAND ----------

# DBTITLE 1,Function to check whether all data from source got copied into sink dataset
def matchData (source_dataset, sink_dataset):
    difference_data = source_dataset.exceptAll(sink_dataset)
    if difference_data.count()> 0:
        return False
    else:
        return True

# COMMAND ----------

# DBTITLE 1,Function to check whether all columns from source got copied into sink dataset
def matchColumns (source_dataset, sink_dataset):
    
    missing = list(set(source_dataset.columns) ^ set(source_dataset.columns))
    if len(missing)>0:
        return False
    else:
        return True

# COMMAND ----------

# DBTITLE 1,Call above functions
if matchRows(source_DF, sink_DF):
    print("Count of rows match between source and sink")
else:
    print("Count of rows doesn't match between source and sink")
    
if matchData(source_DF, sink_DF):
    print("Data match between source and sink")
else:
    print("Data does not match between source and sink")

if matchColumns(source_DF, sink_DF):
    print("Columns match between source and sink")
else:
    print("Columns doesn't match between source and sink")
