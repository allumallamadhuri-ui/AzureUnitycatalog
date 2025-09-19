# Databricks notebook source
# MAGIC %md
# MAGIC ### Scenario1
# MAGIC **- Managed Catalog**
# MAGIC **- Managed Schema**
# MAGIC **- Managed Table**

# COMMAND ----------

# MAGIC %md
# MAGIC **Managed catalog**

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog man_cata

# COMMAND ----------

# MAGIC %md
# MAGIC **managed schema**

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema man_cata.man_schema

# COMMAND ----------

# MAGIC %md
# MAGIC **managed table**

# COMMAND ----------

# MAGIC %sql
# MAGIC create table man_cata.man_schema.man_table (id int, name string)
# MAGIC using delta

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2
# MAGIC **external catalog**
# MAGIC **-managed schema**
# MAGIC **-managed table**

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog ext_cata
# MAGIC managed location 'abfss://mycontainer@storagemoderndbmadhuri.dfs.core.windows.net/external_catalog'

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema ext_cata.man_schema

# COMMAND ----------

# MAGIC %sql
# MAGIC create table ext_cata.man_schema.man_tablez (id int, name string)
# MAGIC using delta

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 3
# MAGIC **external catalog**
# MAGIC **-external schema**
# MAGIC **-managed table**

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema ext_cata.ext_schema
# MAGIC managed location 'abfss://mycontainer@storagemoderndbmadhuri.dfs.core.windows.net/external_schema'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table ext_cata.ext_schema.man_tablez (id int, name string)
# MAGIC using delta

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 4
# MAGIC **External table**

# COMMAND ----------

# MAGIC %sql
# MAGIC create table man_cata.man_schema.ext_table
# MAGIC (
# MAGIC   id int,
# MAGIC   name string
# MAGIC )
# MAGIC using delta
# MAGIC location 'abfss://mycontainer@storagemoderndbmadhuri.dfs.core.windows.net/ext_table/managed_table3'

# COMMAND ----------

# MAGIC %md
# MAGIC #### drop managed table

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table man_cata.man_schema.man_table

# COMMAND ----------

# MAGIC %md
# MAGIC #### undrop managed table

# COMMAND ----------

# MAGIC %sql
# MAGIC undrop table man_cata.man_schema.man_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### querying files using select

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into man_cata.man_schema.ext_table
# MAGIC values (1, 'madhuri'), (2, 'allumalla'), (3, 'Data Engineer')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from man_cata.man_schema.ext_table

# COMMAND ----------

# MAGIC %md
# MAGIC ####Permanent views

# COMMAND ----------

# MAGIC %sql
# MAGIC create view man_cata.man_schema.perm_view
# MAGIC as
# MAGIC select * from delta. `abfss://mycontainer@storagemoderndbmadhuri.dfs.core.windows.net/ext_table/managed_table3` where id =3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from man_cata.man_schema.perm_view

# COMMAND ----------

# MAGIC %md
# MAGIC #### Temporary views

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view temp_view
# MAGIC as
# MAGIC select * from delta. `abfss://mycontainer@storagemoderndbmadhuri.dfs.core.windows.net/ext_table/managed_table3` where id =1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Volumes

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating directory for volume**

# COMMAND ----------

dbutils.fs.mkdirs('abfss://mycontainer@storagemoderndbmadhuri.dfs.core.windows.net/volumes')

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating volume**

# COMMAND ----------

# MAGIC %sql
# MAGIC Create external volume man_cata.man_schema.ext_volume
# MAGIC location 'abfss://mycontainer@storagemoderndbmadhuri.dfs.core.windows.net/volumes'

# COMMAND ----------

# MAGIC %md
# MAGIC **copy file from volume**

# COMMAND ----------

dbutils.fs.cp('abfss://mycontainer@storagemoderndbmadhuri.dfs.core.windows.net/source/ecommerce_orders_large.csv','abfss://mycontainer@storagemoderndbmadhuri.dfs.core.windows.net/volumes/ecommerce_orders_large.csv')

# COMMAND ----------

df = spark.read.option("header", "true").csv(
    "/Volumes/man_cata/man_Schema/ext_volume/ecommerce_orders_large.csv"
)

df.write.format("delta").saveAsTable("man_cata.man_Schema.ecommerce_orders_delta")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM man_cata.man_Schema.ecommerce_orders_delta;
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC