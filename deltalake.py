# Databricks notebook source
# MAGIC %md
# MAGIC ###Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE man_cata.man_schema.deltatbl (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     city STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://mycontainer@storagemoderndbmadhuri.dfs.core.windows.net/delta_lake/deltatbl';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####Turn off deletion vectors

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table man_cata.man_schema.deltatbl set tblproperties ('delta.enabledeletionvectors' = false)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into man_cata.man_schema.deltatbl 
# MAGIC values
# MAGIC (1,'aa','vizag'),
# MAGIC (2,'bb','america'),
# MAGIC (3, 'cc','india')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended man_cata.man_schema.deltatbl

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from man_cata.man_schema.deltatbl

# COMMAND ----------

# MAGIC %md
# MAGIC **CRUD operations**

# COMMAND ----------

# MAGIC %md
# MAGIC ######delta lake use your parquet format data
# MAGIC ######crc and json files are in delta log
# MAGIC ######these json files are holding our transactions
# MAGIC ######create log of all the files
# MAGIC ######create changes in an ascending order
# MAGIC ######what it actually does is create versions

# COMMAND ----------

# MAGIC %md
# MAGIC **update** information from vizag to toronto **in delta table**

# COMMAND ----------

# MAGIC %sql
# MAGIC update man_cata.man_schema.deltatbl
# MAGIC set city = 'toronto' where id = 1

# COMMAND ----------

# MAGIC %md
# MAGIC - we will have 2 files in delta table
# MAGIC - we have only 1 partition earlier
# MAGIC - rewritten that parquet file with change
# MAGIC - 1 has old records
# MAGIC - old file is tombstoned
# MAGIC - 2 has all changes
# MAGIC - in json file it says remove old file and add new file

# COMMAND ----------

# MAGIC %md
# MAGIC **Versioning**

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history man_cata.man_schema.deltatbl

# COMMAND ----------

# MAGIC %md
# MAGIC **Time travel**

# COMMAND ----------

# MAGIC %sql
# MAGIC restore man_cata.man_schema.deltatbl to version as of 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from man_cata.man_schema.deltatbl

# COMMAND ----------

# MAGIC %md
# MAGIC **DELETION VECTOR**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE man_cata.man_schema.deltatbl2 (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     city STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://mycontainer@storagemoderndbmadhuri.dfs.core.windows.net/delta_lake/deltatbl2';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into man_cata.man_schema.deltatbl2 
# MAGIC values
# MAGIC (1,'aa','vizag'),
# MAGIC (2,'bb','america'),
# MAGIC (3, 'cc','india')

# COMMAND ----------

# MAGIC %sql
# MAGIC update man_cata.man_schema.deltatbl2
# MAGIC set city = 'SEATTLE' where id = 1

# COMMAND ----------

# MAGIC %md
# MAGIC - UPDATE ID =1 IS STORED AS REMOVED OR CHANGED
# MAGIC - 1 RECORD IS CHANGED
# MAGIC - ONLY THE CHANGED ONE IT WROTE
# MAGIC - 1ST PARTITION- FLAG THE RECORDS AS CHANGE
# MAGIC - 2ND -JUST INSERTED NEW RECORDS
# MAGIC - Remove the partition in json
# MAGIC - added the record
# MAGIC - it just has updated record
# MAGIC - it has added the deletion vector in the file
# MAGIC - add the same file again which will tell us which record to be removed
# MAGIC
# MAGIC why we have 3rd partition in json known for optimization
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Optimize**

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history man_cata.man_schema.deltatbl2

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize man_cata.man_schema.deltatbl2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deep clone vs shallow clone in delta tables
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE man_cata.man_schema.deepclonetbl
# MAGIC DEEP CLONE man_cata.man_schema.deltatbl

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM man_cata.man_schema.deepclonetbl

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM man_cata.man_schema.deltatbl

# COMMAND ----------

# MAGIC %sql DELETE FROM man_cata.man_schema.deepclonetbl
# MAGIC WHERE id IN (
# MAGIC     SELECT id
# MAGIC     FROM man_cata.man_schema.deepclonetbl
# MAGIC     ORDER BY id DESC
# MAGIC     LIMIT 3
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM man_cata.man_schema.deltatbl

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM man_cata.man_schema.deltatbl2

# COMMAND ----------

# MAGIC %md
# MAGIC **Shalllow clone**

# COMMAND ----------

# MAGIC %sql
# MAGIC create table man_cata.man_schema.shallowtbl
# MAGIC shallow clone man_cata.man_schema.man_table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended man_cata.man_schema.shallowtbl

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC