# Databricks notebook source
# MAGIC %md ## ADLS setting

# COMMAND ----------

storage_account_name = "adlsglobantpoc"
storage_account_access_key = "bre+fddIXPLfnpixCAA07xE98e0ssIxSqNE2kuVh/mxTzILFcePtz72CR6iK8bhD/Z4AhVo+ioND+AStZWhDHQ=="

dbutils.fs.mount(
  source = "wasbs://poc@adlsglobantpoc.blob.core.windows.net".format(storage_account_name),
  mount_point = "/mnt/poc",
  extra_configs = {
    "fs.azure.account.key.adlsglobantpoc.blob.core.windows.net".format(storage_account_name): storage_account_access_key
  }
)

# COMMAND ----------

dbutils.fs.ls("/mnt/poc")

# COMMAND ----------

# MAGIC %md ##SQL Server setting

# COMMAND ----------

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

url = 'jdbc:sqlserver://sqlglobantpoc.database.windows.net:1433;database=GlobantPoC;user=admindb@sqlglobantpoc;password=Medellin01*;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30'

# COMMAND ----------

test_table = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", 'dbo.jobs')
  .load()
)

# COMMAND ----------

display(test_table)
