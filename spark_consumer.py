from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType,StructField, StringType, DateType
from pyspark.sql.functions import struct,col,from_json,to_json,to_timestamp,current_timestamp,date_format
from imply_sdk import ImplyAuthenticator, ImplyEventApi


ORGNAME="implyhackathon"
CLIENT_ID="hackathon_client"
CLIENT_SECRET="8057e539-d566-4b05-bbc3-adb34a2eed6b"
TABLE_ID="edf3f26d-9267-4065-87a9-9c3c434991de" 
KAFKA_BROKERS="kafka-0.kafka-headless.default.svc.cluster.local:9092"
KAFKA_TOPIC="meetup_events"
SPARK_MASTER="spark://spark-master-svc:7077"

# Authenticated with Imply Polaris
auth = ImplyAuthenticator(
    org_name=ORGNAME,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET
)
auth.authenticate()  # get and set the bearer access token

def pushEventBatch( batchDF, epochID):
  event_api = ImplyEventApi(auth=auth, table_id=TABLE_ID)
  data = [str(row['message_out']) for row in batchDF.collect()]
  print(f"data:-->{data}<--")
  response = event_api.push_list(messages=data)
  return response

spark = SparkSession \
    .builder \
    .master(SPARK_MASTER) \
    .appName("KafkaToPolaris_MeetupEvents") \
    .getOrCreate()

#meetup_events source schema
schema = StructType([ 
    StructField("city",StringType(),True), 
    StructField("event_name",StringType(),True), 
    StructField("date",StringType(),True), 
    StructField("host_company", StringType(), True)
  ])

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
  .option("subscribe", KAFKA_TOPIC) \
  .option("startingOffsets", "earliest") \
  .load() \
  .withColumn("value", col("value").cast("string")) \
  .select(from_json(col("value"), schema).alias("message"))

# add __time column to message structure required for Polaris Push Event API
# form the json data that matches the table schema
dfOutputMessages = df.select(
                     to_json(struct(
                       date_format(current_timestamp(),"yyyy-MM-dd hh:mm:ss").alias("__time"),
                       col("message.city").alias("city"),
                       col("message.event_name").alias("event_name"),
                       col("message.host_company").alias("host_company"),
                       col("message.date").alias("date")
                     )).alias("message_out"))

# use forEachBatch as action to send events, bundle batch into single API call
query = (
  dfOutputMessages
  .writeStream
  .foreachBatch(pushEventBatch)
  .outputMode("append")
  .start()
  )
# query = dfOutputMessages.writeStream \
#  .format("kafka") \
#  .option("kafka.bootstrap.servers", "kafka-0.kafka-headless.default.svc.cluster.local:9092") \
#  .option("topic", "meetup_events_out") \
#  .option("checkpointLocation", "/tmp/checkpoint2") \
#  .start()


query.awaitTermination()



