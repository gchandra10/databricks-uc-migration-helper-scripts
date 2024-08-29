# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Assumptions
# MAGIC
# MAGIC - UCX Assessment job run is successful.
# MAGIC - User executing this notebook has privileges to create External Locations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process
# MAGIC
# MAGIC - Storage Credential via Widget or Task Parameter.
# MAGIC - User or Groups (comma separated) who need access to these External Locations.

# COMMAND ----------

# DBTITLE 1,Setup Libraries
# Custom Py file
from common_functions import logger_setup

app_name = "validate_ucx_deployment"
logger,file_handler = logger_setup(app_name)

# COMMAND ----------

# DBTITLE 1,Initialize Widgets
# Initiaze_widgets is not needed when this notebook is triggered as workflow.

# def initialize_widgets():
#     lst_widgets = ["storage_credential", "user_or_groups"]
#     for widget_name in lst_widgets:
#         dbutils.widgets.text(f"{widget_name}", "")


# initialize_widgets()

# COMMAND ----------

# DBTITLE 1,Validating Log file creation
logger.error("Test")

# COMMAND ----------

# DBTITLE 1,Main Routine
if __name__ == "__main__":
    
    # Accept the values as part of Task parameters.

    storage_credential = dbutils.widgets.get("storage_credential")
    user_or_groups = dbutils.widgets.get("user_or_groups")
    cloud = "AZURE" ## "AWS" or "GCP"

    if storage_credential is None:
        logger.error("Missing Storage Credential")
        exit

    if user_or_groups is None:
        logger.error("Missing User/Groups")
        exit

    try:
        # Validate ucx.mounts exists
        mount_table_name = "hive_metastore.ucx.mounts"
        assert spark.catalog.tableExists(mount_table_name), f"Table {mount_table_name} does not exist."

        lst_mnt = spark.sql("select * from hive_metastore.ucx.mounts").collect()

        if lst_mnt:
            for m in lst_mnt:
                mount_name = m.name.replace("/mnt/", "")
                url = m.source

                # Execute this only if the mount is Azure Blob Storage ADLS Gen2 or AWS or GCP
                # Skip in case of wasbs:// mount, as wasbs:// is not supported by UC.

                if (cloud == "AZURE" and url.startswith("abfss://")) or cloud in ["AWS", "GCP"]:
                    try:
                        sql = f"""CREATE EXTERNAL LOCATION IF NOT EXISTS `{mount_name}` URL '{url}' WITH (STORAGE CREDENTIAL `{storage_credential}` ) COMMENT 'Created using Script' """

                        spark.sql(sql)
                        
                        for user in user_or_groups.split(","):
                            psql = f"""GRANT `ALL PRIVILEGES` ON EXTERNAL LOCATION `{mount_name}` TO `{user}` """
                            spark.sql(psql)

                    except Exception as e:
                        # Do not stop the process
                        logger.info(sql)
                        logger.error(e)
                        logger.info("-" * 100)
                        pass
        else:
            logger.info("No mounts found in hive_metastore.ucx.mounts.")
    except Exception as e:
        print(e)

# COMMAND ----------

file_handler.close()
logger.removeHandler(file_handler)
