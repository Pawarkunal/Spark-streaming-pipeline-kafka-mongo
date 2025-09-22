import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col,lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.streaming.state import GroupStateTimeout
from datetime import datetime
from typing import Iterator, Tuple
import logging, subprocess

# --------- logging setup ------------------
logfile_name = f"<LOCAL_PATH_NAME>/{datetime.now().strftime('%Y%m%dT%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(logfile_name)
    ]
)

logger = logging.getLogger(__name__)

# ------------ Intialize spark session --------------
try:
    spark = SparkSession.builder \
        .appName("StatefulOrderPaymentStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.mongodb.spark:mongo-spark-connector_2.12:10.5.0") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.streaming.kafka.maxRatePerPartition", "2") \
        .config("spark.default.parallelism", "2") \
        .getOrCreate()

except Exception as e:
    logger.error(f"Could not initialize spark session: {e}")
    raise

# -------- Define schema for orders and payments ----------
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", IntegerType(), True)
])

payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("payment_date", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("amount", IntegerType(), True)
])

# ------------- Read Order Stream ------------------
try:
    order_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "<your_kafka_server>") \
        .option("subscribe", "<your_topic_name>") \
        .option("startingOffsets", "latest") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.plain.PlainLoginModule required username='<your_username>' password='<your_password>';") \
        .load() \
        .selectExpr("CAST(value as STRING) as value") \
        .select(from_json(col("value"), order_schema).alias("data")) \
        .select("data.*") \
        .withColumn("type", lit("order"))

    logger.info("Order read stream started.....")

except Exception as e:
    logger.error(f"Order stream read failed: {e}")
    raise

# ------------ read payment stream ----------
try:
    payment_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "<our_kafka_server>") \
        .option("subscribe", "<your_topic_name>") \
        .option("startingOffsets", "latest") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.plain.PlainLoginModule required username='<your_username>' password='<your_password>';") \
        .load() \
        .selectExpr("CAST(value as STRING) as value") \
        .select(from_json(col("value"), payment_schema).alias("data")) \
        .select("data.*") \
        .withColumn("type", lit("payment"))

    logger.info("Payment read data stream started.........")

except Exception as e:
    logger.error(f"Payment stream read failed: {e}")

#--------------- combine streams -----------------
try:
    combined_stream = order_stream.unionByName(payment_stream, allowMissingColumns=True)
    logger.info(f"combined stream object: {combined_stream}")
    logger.info(f"Combined stream schema: {combined_stream.printSchema()}")
except Exception as e:
    logger.error(f"Failed to combine streams: {e}")

# Custom function for state management
def process_stateful(
        key: Tuple[str], pdfs: Iterator[pd.DataFrame], state
) -> Iterator[pd.DataFrame]:
    """
    Processes grouped data for a given order_id with stateful logic.

    Args:
        key: The `order_id` as a tuple.
        pdfs: Iterator of pandas DataFrames for the grouped records.
        state: GroupState object for the current key.

    Returns:
        Iterator of pandas DataFrames with joined results or state expiry.
    """

    # Unpack the key
    (order_id,) = key
    logger.info(f"Current key for process: {key}")

    # Get the current state or intialize
    if state.exists:
        (order_date, created_at, customer_id, order_amount) = state.get
    else:
        order_date, created_at, customer_id, order_amount = None, None, None, None

    output_data = []

    logger.info(f"Iterable dataframes for process: {pdfs}")

    # Iterate through the PDFs (grouped data)
    for pdf in pdfs:
        logger.info(f"pdf dataframe check: {pdf.head().to_string()}")

        # Vectorized: Split batch into orders and payments
        orders_df = pdf[pdf["type"] == "order"]
        payments_df = pdf[pdf["type"] == "payment"]

        # Handle orders (process only first order if multiple orders)
        if not orders_df.empty:
            if state.exists:
                # If state is exists then log and ignore the duplicate order
                logger.warning(f"Duplicate order received for order_id={order_id}. Ignoring....")

            else:
                try:
                    # Take the first order
                    first_order = orders_df.iloc[0]

                    #Update the state with all relevant order details
                    order_date = first_order["order_date"]
                    created_at = first_order["created_at"]
                    customer_id = first_order["customer_id"]
                    order_amount = first_order["amount"]

                    order_tup = (order_date, created_at, customer_id, order_amount)
                    logger.info(f"Processing order event: {order_tup}")

                    state.update(order_tup)
                    state.setTimeoutDuration(15 * 60 * 1000) # set 15 min timeout

                    # Log if extra orders for same order_id
                    if len(orders_df) > 1:
                        logger.warning(f"Multiple orders ({len(orders_df) - 1 } extras) for order_id: {order_id} in this batch. Ignoring extras.")
                
                except (IndexError, KeyError) as e:
                    logger.error(f"Error processing orders for order_id: {order_id} : {e}. Skipping.")
                    continue
        
        # Handles payments (process only the first if state exists; ignore others)
        if not payments_df.empty:
            if not state.exists:
                # If no corrosponding order then log and ignore the payment
                logger.warning(f"Payment received for unknown order_id = {order_id}. ignoring...")
            
            else:
                try:
                    # Take only first payment
                    first_payment = payments_df.iloc[0]

                    payment_date = first_payment["payment_date"]
                    payment_amount = first_payment["amount"]
                    payment_id = first_payment["payment_id"]

                    payment_tup = (payment_date, payment_amount, payment_id)
                    logger.info(f"Processing payment event: {payment_tup}")

                    # Appened the joined data to first payment only
                    output_data.append({
                        "order_id": order_id,
                        "order_date": order_date,
                        "created_at": created_at,
                        "customer_id": customer_id,
                        "order_amount": order_amount,
                        "payment_id": payment_id,
                        "payment_date": payment_date,
                        "payment_amount": payment_amount
                    })

                    # Log if extra payments for same order_id
                    if len(payments_df) > 1:
                        logger.warning(f"Multiple payments ({len(payments_df) -1 } extras) for order_id: {order_id} in this batch. Ignoring extras.")

                    # remove the state after successful join
                    logger.info(f"Removing state for order_id: {order_id} after successful join.")
                    state.remove()
                
                except (IndexError, KeyError) as e:
                    logger.error(f"Error processing payments for order_id: {order_id} : {e}. Skipping.")
                    continue
    
    # Handle state Timeout
    if state.hasTimedOut:
        logger.warning(f"State for order_id: {order_id} has timed out. Cleaning up.")
        state.remove()

    # Return as an iterator of pandas DataFrames
    return iter([pd.DataFrame(output_data)]) if output_data else iter([])

# Apply stateful processing with applyInPandasWithState
try:
    stateful_query = combined_stream.groupBy("order_id").applyInPandasWithState(
        func=process_stateful,
        outputStructType="""
            order_id STRING,
            order_date STRING,
            created_at STRING,
            customer_id STRING,
            order_amount INT,
            payment_id STRING,
            payment_date STRING,
            payment_amount INT
        """,
        stateStructType="""
            order_date STRING,
            created_at STRING,
            customer_id STRING,
            amount INT
        """,
        outputMode="append",
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
    )

    logger.info("State update query started.......")

except Exception as e:
    logger.error(f"Failed to set up stateful query: {e}")
    raise

try:
    checkpoint_dir = "gs://kafka-mongo-stateful-streaming/stateful_streaming_logs/"

    # write output to console for testing
    # query = stateful_query.writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .option("checkpointLocation", checkpoint_dir) \
    #     .start()

    # Writing output to mongoDB
    query = stateful_query.writeStream \
        .outputMode("append") \
        .format("mongodb") \
        .option("spark.mongodb.connection.uri","mongodb+srv://<your_mongodb_username>:<your_mongodb_password>@mongoclusternew.2bbu3gv.mongodb.net/?retryWrites=true&w=majority&appName=MongoClusterNew") \
        .option("spark.mongodb.database","<database_name>") \
        .option("spark.mongodb.collection","<collection_name>") \
        .option("truncate", False) \
        .option("checkpointLocation", checkpoint_dir) \
        .start()

    logger.info("Writing schema to MongoDB started.....")

except Exception as e:
    logger.error(f"failed to start streaming query: {e}")
    raise

try:
    query.awaitTermination()
except Exception as e:
    logger.error(f"Streaming query termination failed: {e}")
finally:
    local_logfile = logfile_name
    gcs_destination = "<Checkpoint GCS bucket location>"
    try:
        subprocess.run(["gsutil" , "cp", local_logfile, gcs_destination], check=True)
        logger.info(f"Logfile uploaded to GCS: {gcs_destination}")
    except Exception as e:
        logger.error(f"Log upload to GCS failed: {e}")
