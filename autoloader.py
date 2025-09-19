# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Autoloader

# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming dataframe**

# COMMAND ----------

df = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .option("cloudFiles.schemaLocation", "abfss://mycontainer@storagemoderndbmadhuri.dfs.core.windows.net/autosink/check")
      .load("abfss://mycontainer@storagemoderndbmadhuri.dfs.core.windows.net/autosource"))

# COMMAND ----------

df.writeStream.format("parquet")\
         .option("checkpointLocation", "abfss://mycontainer@storagemoderndbmadhuri.dfs.core.windows.net/autosink/check")\
         .trigger(processingTime="10 seconds")\
         .start("abfss://mycontainer@storagemoderndbmadhuri.dfs.core.windows.net/autosink/data")