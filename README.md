# UC Migration Helper Scripts

To migrate from HMS to UC, [UCX](https://github.com/databrickslabs/ucx) should be the goto tool. The scripts found in this repo can be used as supplement if needed.

---

**Notebook: azure_get_location_protocol**

Used to identify the storage protocols in Azure cloud.

UC does not support wasbs://

If wasbs:// protocol is used, create equivalent containers / storage locations using abfss:// and copy the data from wasbs:// to abfss://.

```azcopy``` works better. In case of smaller datasets, read the data from wasbs:// using SPARK and write to abfss://.

---

**Notebook: create-external-location-from-mount**

Used to create External locations from available Mounts. 

Pre-req: UCX assessment workflow is executed and output available in *hive_metastore.ucx.mounts*

---

**Notebook: uc-hms-compare-tables-views**

Many times Engineers will continue to create or drop Databases / Tables when the UC migration is in-progress. This script will help to identify those changes.

---

**Notebook: convert-low-level-rdd-to-dataframe**

Contains Non-RDD equivalent code snippets for RDD/Low-Level scripts.




