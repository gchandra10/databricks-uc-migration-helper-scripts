# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Using **Azure cloud**, use this to determine how many databases, tables, mounts are using wasbs:// protocol.
# MAGIC
# MAGIC - This can be used prior to running UCX assessment.
# MAGIC - If wasbs:// protocol is used, create equivalent containers / storage locations using abfss:// and copy the data from wasbs:// to abfss://
# MAGIC - *azcopy* tool works better. In case of smaller datasets, read the data from wasbs:// using SPARK and write to abfss://

# COMMAND ----------

# DBTITLE 1,Import Libraries
from typing import List,Dict
from pyspark.sql.functions import col

# Custom Py file
from common_functions import logger_setup

app_name = "azure_get_location_protocol"
logger,file_handler = logger_setup(app_name)

# COMMAND ----------

# DBTITLE 1,Testing Logger
logger.error("Another Test1")

# COMMAND ----------

# DBTITLE 1,Get Mount Points
# returns all the mount point names as a dictionary. 

def get_mount_points() -> Dict[str, str]:
    """
        This function returns list of Mount Point names along with Cloud Location
        /mnt/mountname  abfs://cloud_path
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
        return spark.catalog.listDatabases()
    except Exception as e:
        logger.error(e)

# COMMAND ----------

# DBTITLE 1,Get Table Mount Location
def get_table_mount_location():
    """
        This function returns all the Tables from each database along with mount location and adls_prefix
        The adls_prefix can be wasbs or abfss.
        Unity Catalog doesn't support wasbs.
    """
    logger.info("Executing get_table_mount_location function")

    # Collect all the mount points as a Dictionary
    # The tables created will have Mount Point location /mnt/somename
    # This function will help to replace the /mnt/somename with equivalent ADLS location
    mount_dict = get_mount_points()

    # Collect all databases as List
    all_databases = get_all_databases()

    lst_final_data = []
    try:
        for database in all_databases:
            #print(dbs.name)
            #spark.sql(f"use hive_metastore.{database.name}")
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
                                    loc_parts = location.split('/')
                                    # retrieving the first 2 part of the path /mnt/mountname
                                    required_path = '/'+ '/'.join(loc_parts[1:3])

                                    row = (database.name,t.tableName,required_path, mount_dict.get(required_path))

                                    lst_final_data.append(row)
                            except Exception as e:
                                logger.error(e)
                                # When error happens, don't stop the process continue
                                pass
            except Exception as e:
                logger.error(e)

        columns = ["database_name","table_name","location","adls_prefix"]
        if len(lst_final_data) > 0:
            df = spark.createDataFrame(lst_final_data,columns)

        return df
    
    except Exception as e:
        logger.error(e)

# COMMAND ----------

# DBTITLE 1,Main Routine
if __name__ == "__main__":
    try:
        df = get_table_mount_location()

        # Filter wasbs protocols
        df_filter = df.filter(col("adls_prefix") == "wasbs")

        if df_filter.count() > 0:
            print("wasbs protocols are not supported by Unity Catalog, please move the data to abfss:// and remount wasbs:// to abfss://. Migration will EXIT now.")
            display(df_filter)
            exit
    except Exception as e:
        logger.error(e)

# COMMAND ----------

file_handler.close()
logger.removeHandler(file_handler)

# COMMAND ----------


