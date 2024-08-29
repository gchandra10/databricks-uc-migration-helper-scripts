# UC Migration Helper Scripts

To migrate from HMS to UC, [UCX](https://github.com/databrickslabs/ucx) should be the goto tool. The scripts found in this repo can be used as supplement if needed.

---

**Script: azure_get_location_protocol**

Used to identify the storage protocols in Azure cloud.

UC does not support wasbs://

If wasbs:// protocol is used, create equivalent containers / storage locations using abfss:// and copy the data from wasbs:// to abfss://.

```azcopy``` tool works better. In case of smaller datasets, read the data from wasbs:// using SPARK and write to abfss://.

---

**Script: create-external-location-from-mount**

Used to create External locations from available Mounts. 

Pre-reg: UCX assessment workflow is executed and output available in *hive_metastore.ucx.mounts*

---
