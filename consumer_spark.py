from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StringType, TimestampType

# 1. Create SparkSession
spark = SparkSession.builder \
    .appName("BankTransactionReconciliation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define schema for both topics
common_schema = StructType() \
    .add("txn_id", StringType()) \
    .add("amount", StringType()) \
    .add("timestamp", TimestampType())

# 3. Read from Kafka: gateway topic
gateway_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "gateway-transactions") \
    .option("startingOffsets", "latest") \
    .load()

gateway_df = gateway_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), common_schema).alias("data")) \
    .select("data.*") \
    .withWatermark("timestamp", "1 minute")

# 4. Read from Kafka: ledger topic
ledger_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ledger-transactions") \
    .option("startingOffsets", "latest") \
    .load()

ledger_df = ledger_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), common_schema).alias("data")) \
    .select("data.*") \
    .withWatermark("timestamp", "1 minute")

# 5. Perform full outer join on txn_id and timestamp
joined_df = gateway_df.alias("g").join(
    ledger_df.alias("l"),
    expr("""
        g.txn_id = l.txn_id AND 
        g.timestamp = l.timestamp
    """),
    "fullouter"
)

mismatched_df = joined_df.filter(
    (col("g.txn_id").isNull()) | (col("l.txn_id").isNull())
)



cleaned_df = mismatched_df.selectExpr(
    "coalesce(g.txn_id, l.txn_id) as txn_id",
    "coalesce(g.amount, l.amount) as amount",
    "coalesce(g.timestamp, l.timestamp) as timestamp",
    "g.txn_id is null as missing_in_gateway",
    "l.txn_id is null as missing_in_ledger"
)


# 7. snowflake connection option 
sfOptions = {
    "sfURL": "qavbvlj-zz40198.snowflakecomputing.com",  
    "sfUser": "BALODAPREETAM",
    "sfPassword": "demgehWajvehnizga8",
    "sfDatabase": "FinReconDB",
    "sfSchema": "TransactionSchema",
    "sfWarehouse": "FinReconWH",
    "sfRole": "ACCOUNTADMIN"
}

# 8. Write mismatched records to Snowflake
def write_to_snowflake(batch_df, epoch_id):
    batch_df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**sfOptions) \
        .option("dbtable", "mismatched_transactions") \
        .mode("append") \
        .save()

query = cleaned_df.writeStream \
    .foreachBatch(write_to_snowflake) \
    .option("checkpointLocation", "/tmp/snowflake_checkpoint") \
    .start()

import time
while query.isActive:
    print(query.lastProgress)
    time.sleep(10)

print("✅ Spark Streaming started... waiting for data.")

query.awaitTermination()

