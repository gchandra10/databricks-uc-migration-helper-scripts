# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC In HMS world its common to create multiple tables pointing to the same External location. This script will help to identify those tables.

# COMMAND ----------

# DBTITLE 1,Import Libraries
from typing import List,Dict
from pyspark.sql.functions import col

# Custom Py file
from common_functions import logger_setup

app_name = "identify-tables-with-same-location"
logger,file_handler = logger_setup(app_name)

# COMMAND ----------

# DBTITLE 1,Get Mount Points
# returns all the mount point names as a dictionary. 

def get_mount_points() -> Dict[str, str]:
    """
        This function returns list of Mount Point names along with Cloud Location
        /mnt/mountname  abfs://cloud_path  s3://
    """
    try:
        logger.info("Executing get_mount_points function")
        
        for m in dbutils.fs.mounts():
            mount_dict = {m.mountPoint:m.source[:5] for m in dbutils.fs.mounts()}
        
        logger.info("Mount points collected")
        return mount_dict
    except Exception as e:
        logger.error(e)

# COMMAND ----------

# DBTITLE 1,Get All Databases
## returns all databases under hive_metastore catalog

def get_all_databases() -> List[str]:
    """
        This function returns the list of all databases under hive_metastore
    """
    try:
        logger.info("Executing get_all_databases function")
        
        spark.sql("use catalog hive_metastore")
        #return spark.catalog.listDatabases()
        return [spark.catalog.getDatabase("uc_demo")]
    except Exception as e:
        logger.error(e)

# COMMAND ----------

# DBTITLE 1,Get Table Mount Location
def get_table_mount_location():
    """
        This function returns all the Tables from each database along with location
    """
    logger.info("Executing get_table_mount_location function")

    # mount_dict = get_mount_points()

    # Collect all databases as List
    all_databases = get_all_databases()

    lst_final_data = []
    try:
        for database in all_databases:
            logger.info(f"Collecting data from hive_metastore.{database.name}")

            lst_tables = spark.sql(f"SHOW TABLES IN hive_metastore.{database.name}").collect()
            try:
                for t in lst_tables:
                    if t.isTemporary is False:
                        ## Skip __apply tables created by DLT
                        if not t.tableName.startswith('__'):
                            try:
                                sql = f"describe extended hive_metastore.`{database.name}`.`{t.tableName}`"
                                extended_desc = spark.sql(sql)

                                location_info = extended_desc.filter(extended_desc["col_name"]=="Location").collect()
                                if location_info:
                                    location = location_info[0]["data_type"]
                                    row = (database.name,t.tableName,location)

                                    lst_final_data.append(row)
                            except Exception as e:
                                logger.error(e)
                                # When error happens, don't stop the process continue
                                pass
            except Exception as e:
                logger.error(e)

        columns = ["database_name","table_name","location"]

        if len(lst_final_data) > 0:
            df = spark.createDataFrame(lst_final_data,columns)

        return df
    
    except Exception as e:
        logger.error(e)

# COMMAND ----------

# DBTITLE 1,Main Routine
from pyspark.sql.window import Window
from pyspark.sql.functions import count, col

if __name__ == "__main__":
    try:
        df = get_table_mount_location()

        window_spec = Window.partitionBy("location") 
        # Add a count column over the window specification
        df_with_counts = df.withColumn("location_count", count("location").over(window_spec))
        
        # Filter where count of location is greater than 1
        df_result = df_with_counts.filter(col("location_count") > 1) \
                       .select("database_name", "table_name", "location", "location_count")

        display(df_result)

    except Exception as e:
        logger.error(e)

# COMMAND ----------

file_handler.close()
logger.removeHandler(file_handler)
