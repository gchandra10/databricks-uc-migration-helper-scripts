# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Assumptions
# MAGIC
# MAGIC - UCX Table Migration is done.
# MAGIC - User executing this notebook has access to UC and HMS.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Case
# MAGIC
# MAGIC - Many times Engineers will continue to create or drop Databases / Tables when Migration is in progress.
# MAGIC This script will help to identify those changes.

# COMMAND ----------

# Custom Py file
from common_functions import logger_setup

app_name = "uc-hms-compare-tables"
logger,file_handler = logger_setup(app_name)

# COMMAND ----------

# DBTITLE 1,Load Databases
def load_databases():
    # Enter the list if selected databases need to be compared. Comment the following if all you need to compare all databases.
    lst_databases_to_check = [
        'db1',
        'db2',
        'db3'
    ]

    #If the list is EMPTY it will compare all DB
    
    if len(lst_databases_to_check) == 0: 
        # Use this to compare all databases
        lst_all_databases = spark.sql("show databases from hive_metastore;").collect()
        lst_databases_to_check = [row.databaseName for row in lst_all_databases]

    return lst_databases_to_check

# COMMAND ----------

def initialize_widgets():
    lst_widgets = ['uc_catalog_name']
    for widget_name in lst_widgets:
        dbutils.widgets.text(f"{widget_name}","") 

initialize_widgets()

# COMMAND ----------

if __name__ == "__main__":
    try:
        uc_catalog_name = dbutils.widgets.get("uc_catalog_name") or None
        lst_databases_to_check = load_databases()
        
        if not uc_catalog_name is None:

            lst_only_in_hms = []
            lst_only_in_uc = []

            for db in lst_databases_to_check:
                
                print(f"Comparing hive_metastore.{db} with {uc_catalog_name}.{db}")
                logger.info(f"Comparing hive_metastore.{db} with {uc_catalog_name}.{db}")

                hms_db = f"hive_metastore.{db}"
                uc_db = f"{uc_catalog_name}.{db}"
                logger.info(f"hms_db:{hms_db} , uc_db:{uc_db}")

                table_names_hms = spark.sql(f"SHOW TABLES IN {hms_db}").select("tableName").collect()
                lst_objects_in_hms_db = [row.tableName for row in table_names_hms]

                if spark.catalog.databaseExists(uc_db):
                    table_names_uc = spark.sql(f"SHOW TABLES IN {uc_db}").select("tableName")
                    lst_objects_in_uc_db = [row.tableName for row in table_names_uc.collect()]
                else:
                    lst_objects_in_uc_db = []

                set_objects_only_in_hms_db = set(lst_objects_in_hms_db) - set(lst_objects_in_uc_db)
                set_objects_only_in_uc_db = set(lst_objects_in_uc_db) - set(lst_objects_in_hms_db)

                #logger.info(f"only in hms {set_objects_only_in_hms_db}")
                #logger.info(f"only in uc {set_objects_only_in_uc_db}")
                
                if len(list(set_objects_only_in_hms_db)) > 0:
                    lst_only_in_hms.append({db:list(set_objects_only_in_hms_db)} )

                if len(list(set_objects_only_in_uc_db)) > 0:
                    lst_only_in_uc.append({db:list(set_objects_only_in_uc_db)} )
            

            if len(lst_only_in_hms) > 0:
                rows = []
                for item in lst_only_in_hms:
                    for key,values in item.items():
                        rows.append((key,values))
                df_only_in_hms = spark.createDataFrame(rows,schema=["db","tables"])

                print("-" * 100)
                print("Objects only in HMS")
                display(df_only_in_hms)

            if len(lst_only_in_uc) > 0:
                rows = []
                for item in lst_only_in_uc:
                    for key,values in item.items():
                        rows.append((key,values))
                df_only_in_uc = spark.createDataFrame(rows,schema=["db","tables"])

                print("Objects only in UC")
                display(df_only_in_uc)

    except Exception as e:
        print(e)
        logger.error(e)


# COMMAND ----------

file_handler.close()
logger.removeHandler(file_handler)
