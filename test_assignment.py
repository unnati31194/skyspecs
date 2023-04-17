# Databricks notebook source
# DBTITLE 1,Install Pytest
# MAGIC %pip install pytest

# COMMAND ----------

# DBTITLE 1,Import Pytest
import pytest

# COMMAND ----------

# DBTITLE 1,Call the functions
tableName = "housing_dataset"
dbName = "practise"
columnList   = ['MedInc', 'HouseAge', 'AveRooms', 'AveBedrms', 'Population', 'AveOccup', 'Latitude', 'Longitude']

sparkDF = spark.read.format("delta").load("/FileStore/tables/unnati/")

# Does the table exist?
def test_tableExists():
    assert tableExists(tableName, dbName) is True

# Does the column exist?
def test_columnExists():
    assert columnExists(sparkDF, columnName) is True

# Is there data in the sink table?
def test_numOfRowsInTheData():
    assert numOfRowsInTheData(sparkDF) > 0
