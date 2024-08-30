# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Use the RDD equivalents on UC Shared

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading Data

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC ## RDD Map & Collect()

# COMMAND ----------

df = (spark.read.format("delta")
      .load("/databricks-datasets/learning-spark-v2/people/people-10m.delta")
      .filter(col("gender") == "F")
)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Rdd Version

# COMMAND ----------

type(df.select("firstName").rdd.map(lambda x:x[0]).collect())

# COMMAND ----------

rdd_lst = df.select("firstName").rdd.map(lambda x:x[0]).collect()
rdd_lst_len = len(rdd_lst)
print(rdd_lst[0:3], rdd_lst_len)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Non RDD Version

# COMMAND ----------

from pyspark.sql.functions import col

df_lst=df.select(col("firstName")).toPandas()["firstName"].tolist()
df_lst_len=len(df_lst)
print(df_lst[0:3], df_lst_len)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flatmap

# COMMAND ----------

data = [{"job_id": 1, "partition_id": 2},{"job_id": 2, "partition_id": 3} ]
df = spark.createDataFrame(data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### RDD Version

# COMMAND ----------

schema_files = df\
    .select("job_id", "partition_id")\
    .distinct()\
    .rdd.flatMap(lambda x: ["file:/dbfs/dbfs/mnt/cdc/checkpoint/mde/dbz_checkpoint_{}/{}".format(x[0],x[1])+"/schema.dat"]).collect()
    
display(schema_files)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Non RDD Version

# COMMAND ----------

from pyspark.sql import functions as F

schema_files = (df
                .select("job_id", "partition_id")
                .distinct()
                .withColumn("schema_file", F.concat(F.lit("file:/dbfs/dbfs/mnt/cdc/checkpoint/mde/dbz_checkpoint_"), F.col("job_id"), F.lit("/"), F.col("partition_id"), F.lit(".schema")))
                .select("schema_file")
                .toPandas()["schema_file"].tolist()
)

display(schema_files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## isEmpty()

# COMMAND ----------

data = []
df = spark.createDataFrame(data,"Int")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### RDD Version

# COMMAND ----------

if df.rdd.isEmpty():
    print("DF is empty")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Non RDD Version

# COMMAND ----------

if df.isEmpty():
    print("DF is empty")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## sc.parallelize List

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### RDD Version

# COMMAND ----------

from pyspark.sql import Row

# Assuming `rdd` is your RDD, for example:
rdd = sc.parallelize([("Alice", 1), ("Bob", 2)])
rdd_row = rdd.map(lambda x: Row(name=x[0], id=x[1]))

df = spark.createDataFrame(rdd_row)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Non RDD Version

# COMMAND ----------

from pyspark.sql import SparkSession, Row

# Create a DataFrame directly from a list of Row objects
data = [("Alice", 1), ("Bob", 2)]
schema = ["name", "id"]

df = spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## sc.parallelize Dictionary

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### RDD Version

# COMMAND ----------

# Example list of dictionaries
data = [{"Name": "Alice", "ID": 1}, {"Name": "Bob", "ID": 2}]

# Convert list to RDD
rdd = spark.sparkContext.parallelize(data)

# Create DataFrame from RDD (schema is inferred)
df = spark.createDataFrame(rdd)

df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Non RDD Version

# COMMAND ----------

from pyspark.sql import SparkSession

# Example list of dictionaries
data = [{"Name": "Alice", "ID": 1}, {"Name": "Bob", "ID": 2}]

# Create DataFrame from list of dictionaries (schema is inferred)
df = spark.createDataFrame(data)

df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## sc.parallelize JSON

# COMMAND ----------

# MAGIC %md
# MAGIC ### RDD Version

# COMMAND ----------

import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

res = requests.get("https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json")

make_df1 = spark.read.json(sc.parallelize([res.text]))

display(make_df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Non RDD Version

# COMMAND ----------

import requests
import json

res = requests.get("https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json")
data = res.json()

make_df1 = spark.createDataFrame(data['Results'])
display(make_df1)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## emptyRDD

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### RDD Version

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# Define schema using DDL string
schemaDDL = """message STRING, 
               db_name STRING, 
               schema_name STRING, 
               table_name STRING, 
               source_ts_ms TIMESTAMP, 
               op STRING, 
               ts_ms TIMESTAMP"""

# Create an empty RDD
emptyRDD = spark.sparkContext.emptyRDD()

# Create a StructType schema from the DDL string
schema = StructType.fromDDL(schemaDDL)

# Create an empty DataFrame using the empty RDD and the schema
emptyDF = spark.createDataFrame(emptyRDD, schema)

# Show the empty DataFrame
emptyDF.show()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Non RDD Version

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# Define schema using DDL string
schemaDDL = """message STRING, 
               db_name STRING, 
               schema_name STRING, 
               table_name STRING, 
               source_ts_ms TIMESTAMP, 
               op STRING, 
               ts_ms TIMESTAMP"""

# Create an empty DataFrame directly using the schema
schema = StructType.fromDDL(schemaDDL)
emptyDF = spark.createDataFrame([], schema)

emptyDF.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# Define schema using DDL string
schemaDDL = """message STRING, 
               db_name STRING, 
               schema_name STRING, 
               table_name STRING, 
               source_ts_ms TIMESTAMP, 
               op STRING, 
               ts_ms TIMESTAMP"""

# Parse the DDL to create a StructType schema
schema = StructType.fromDDL(schemaDDL)

# Generate SQL SELECT expression for each field
selectExpr = ", ".join([
    f"CAST(NULL as {field.dataType.simpleString()}) as `{field.name}`"
    if "timestamp" in field.dataType.simpleString() else f"NULL as `{field.name}`"
    for field in schema.fields
])

# Construct the SQL query to create an empty DataFrame with the specified schema
sqlQuery = f"SELECT {selectExpr} WHERE 1=0"

# Execute the SQL query to create the empty DataFrame
emptyDF = spark.sql(sqlQuery)

# Show the schema of the empty DataFrame
emptyDF.show()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Table/Database Exists

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### RDD Version

# COMMAND ----------

spark._jsparkSession.catalog().databaseExists("catalog.database")

# COMMAND ----------

spark._jsparkSession.catalog().tableExists("catalog.database.table")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Non RDD Version

# COMMAND ----------

spark.catalog.databaseExists("catalog.database")

# COMMAND ----------

spark.catalog.tableExists("catalog.database.table")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## spark._jvm.org.apache.log4j

# COMMAND ----------

# MAGIC %md
# MAGIC ### RDD Version

# COMMAND ----------

logger = spark._jvm.org.apache.log4j
log = logger.LogManager.getLogger("myAppLogger")
log.info("process started")
print("Hello World!")
log.info("process ended")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Non RDD Version

# COMMAND ----------

import logging
# Configure the logging

logging.basicConfig(level=logging.INFO)
#logging.basicConfig(level=logging.ERROR)

logger = logging.getLogger("myAppLogger")

# Print and log the messages
logger.info("process started")
print("Hello World!")
logger.info("process ended")

# COMMAND ----------

# MAGIC %md
# MAGIC ## sparkContext.getConf()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Non Supported Usage

# COMMAND ----------

spark.sparkContext.getConf().getAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Supported Usage

# COMMAND ----------

from dbruntime.databricks_repl_context import get_context
import json

dict_result = get_context().__dict__ 

print(dict_result)

# COMMAND ----------

spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Undocumented Feature

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson()

# COMMAND ----------


