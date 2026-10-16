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
df_raw.count()  # confirm ~100,000

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import random
random.seed(42)

teams = ["Tier 1 Support", "Tier 2 Support", "Billing Specialists", "Security Response"]
first_names = ["Maya","Liam","Sofia","Noah","Elena","Omar","Priya","Lucas","Anya","Ken",
               "Zara","Theo","Nadia","Marco","Hana","Felix","Ivy","Rashid","Clara","Victor"]
last_names  = ["Reyes","Chen","Ibrahim","Novak","Dubois","Patel","Larsen","Costa","Weiss","Osei"]

agents = [{
    "agent_id": f"AGT_{i:03d}",
    "agent_name": f"{random.choice(first_names)} {random.choice(last_names)}",
    "team": random.choice(teams),
    "seniority": random.choices(["Junior","Mid","Senior"], weights=[0.4,0.4,0.2])[0],
} for i in range(1, 31)]

dim_agent = spark.createDataFrame(agents).withColumn("agent_key", F.monotonically_increasing_id())

agent_ids = [a["agent_id"] for a in agents]
assign_udf = F.udf(lambda: random.choice(agent_ids))
df_raw = df_raw.withColumn("agent_id", assign_udf())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sla_targets = []
for plan in ["standard", "gold", "platinum"]:
    for pr in ["low", "medium", "high", "urgent"]:
        base = {"low": 72, "medium": 48, "high": 24, "urgent": 4}[pr]
        mult = {"standard": 1.0, "gold": 0.75, "platinum": 0.5}[plan]
        sla_targets.append({"sla_plan": plan, "priority": pr, "target_hours": base * mult})

dim_sla_target = spark.createDataFrame(sla_targets)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def make_dim(df, col, key_name):
    return df.select(F.col(col)).distinct().na.drop().withColumn(key_name, F.monotonically_increasing_id())

dim_customer_segment = make_dim(df_raw, "customer_segment", "segment_key")
dim_channel          = make_dim(df_raw, "channel", "channel_key")
dim_product          = make_dim(df_raw, "product_area", "product_key")
dim_issue_type       = make_dim(df_raw, "issue_type", "issue_type_key")
dim_priority         = make_dim(df_raw, "priority", "priority_key")
dim_status           = make_dim(df_raw, "status", "status_key")
dim_sla_plan         = make_dim(df_raw, "sla_plan", "sla_plan_key")
dim_sentiment        = make_dim(df_raw, "customer_sentiment", "sentiment_key")
dim_platform         = make_dim(df_raw, "platform", "platform_key")
dim_region           = make_dim(df_raw, "region", "region_key")

min_d, max_d = df_raw.selectExpr("min(created_at)", "max(created_at)").first()
dim_date = spark.sql(f"""
    SELECT explode(sequence(to_date('{min_d.date()}'), to_date('{max_d.date()}'), interval 1 day)) as date
""").withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int")) \
   .withColumn("year", F.year("date")).withColumn("month", F.month("date")) \
   .withColumn("quarter", F.quarter("date"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_tickets = df_raw \
    .join(dim_agent.select("agent_id","agent_key"), "agent_id") \
    .join(dim_customer_segment, "customer_segment") \
    .join(dim_channel, "channel") \
    .join(dim_product, "product_area") \
    .join(dim_issue_type, "issue_type") \
    .join(dim_priority, "priority") \
    .join(dim_status, "status") \
    .join(dim_sla_plan, "sla_plan") \
    .join(dim_sentiment, "customer_sentiment") \
    .join(dim_platform, "platform") \
    .join(dim_region, "region") \
    .join(dim_sla_target, ["sla_plan", "priority"]) \
    .withColumn("date_key", F.date_format("created_at", "yyyyMMdd").cast("int")) \
    .withColumn("sla_breached",
        F.when(F.col("resolution_time_hours").isNotNull() & (F.col("resolution_time_hours") > F.col("target_hours")), 1)
         .when(F.col("resolution_time_hours").isNotNull(), 0)
         .otherwise(None)) \
    .select("ticket_id","date_key","agent_key","segment_key","channel_key","product_key",
            "issue_type_key","priority_key","status_key","sla_plan_key","sentiment_key",
            "platform_key","region_key","resolution_time_hours","target_hours","sla_breached",
            "reopened","csat_score","has_attachment",
            "initial_message","agent_first_reply","resolution_summary")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables = {
    "fact_tickets": fact_tickets, "dim_date": dim_date, "dim_agent": dim_agent,
    "dim_customer_segment": dim_customer_segment, "dim_channel": dim_channel,
    "dim_product": dim_product, "dim_issue_type": dim_issue_type, "dim_priority": dim_priority,
    "dim_status": dim_status, "dim_sla_plan": dim_sla_plan, "dim_sentiment": dim_sentiment,
    "dim_platform": dim_platform, "dim_region": dim_region, "dim_sla_target": dim_sla_target,
}
for name, d in tables.items():
    d.write.format("delta").mode("overwrite").saveAsTable(name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM ServiceDeskDemo.dbo.dim_agent LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM ServiceDeskDemo.dbo.fact_tickets LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
