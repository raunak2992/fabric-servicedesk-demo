# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "206a72bd-b38a-46de-a0dd-1748e17c9821",
# META       "default_lakehouse_name": "ServiceDeskDemo",
# META       "default_lakehouse_workspace_id": "429fa5d3-41ee-48c5-86ec-84dccbce5835",
# META       "known_lakehouses": [
# META         {
# META           "id": "206a72bd-b38a-46de-a0dd-1748e17c9821"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
df_raw = spark.read.option("header", True).option("inferSchema", True) \
    .csv("Files/synthetic_it_support_tickets.csv") \
    .withColumn("created_at", F.to_timestamp("created_at"))
df_raw.printSchema()
df_raw.count()   # expect ~100,000

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
