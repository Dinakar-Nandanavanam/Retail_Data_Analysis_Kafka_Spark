
# Import dependent libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.window import Window



# Initiate Spark Session
spark = SparkSession  \
        .builder  \
        .appName("KafkaSparkRetailAnalysis")  \
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')


# Set bootstrap server and kafka-topic of stream source
bootstrap_server = "18.211.252.152:9092"
kafka_topic = "real-time-project"


# Read Input Streams of Order Data
orderRawStream = spark  \
        .readStream  \
        .format("kafka")  \
        .option("kafka.bootstrap.servers", bootstrap_server)  \
        .option("subscribe", kafka_topic)  \
        .option("startingOffsets", "latest")  \
        .load()


# Define Order Schema
orderSchema = StructType() \
        .add("invoice_no", LongType()) \
	    .add("country",StringType()) \
        .add("timestamp", TimestampType()) \
        .add("type", StringType()) \
        .add("total_items",IntegerType())\
        .add("is_order",IntegerType()) \
        .add("is_return",IntegerType()) \
        .add("items", ArrayType(StructType([
        StructField("SKU", StringType()),
        StructField("title", StringType()),
        StructField("unit_price", FloatType()),
        StructField("quantity", IntegerType()) 
        ])))


# To Parse and obtain Order Stream from Raw Json Streams of Order Data
orderStream = orderRawStream.select(from_json(col("value").cast("string"), orderSchema).alias("orders_df")).select("orders_df.*")


# Define Utility functions

def is_order(type):
    status = 1 if type.upper()=="ORDER" else 0
    return status 

def is_return(type):
    status = 1 if type.upper()=="RETURN" else 0
    return status 
       
def total_item_count(items):
   total_count = 0
   for item in items:
       total_count = total_count + item['quantity']
   return total_count

def total_cost(items,type):
   total_amt = 0
   for item in items:
       total_amt = total_amt + item['unit_price'] * item['quantity']         
   total_price  =  total_amt * (-1 if type.upper()=="RETURN" else 1)
   return total_price



# Define User Defined Functions(UDFs) using utility functions
udf_is_order = udf(is_order, IntegerType())
udf_is_return = udf(is_return, IntegerType())
udf_total_item_count = udf(total_item_count, IntegerType())
udf_total_cost = udf(total_cost, FloatType())



# Generate Order Stream with derived columns using UDFs
expandedOrderStream = orderStream \
       .withColumn("total_items", udf_total_item_count(orderStream.items)) \
       .withColumn("total_cost", udf_total_cost(orderStream.items, orderStream.type) ) \
       .withColumn("is_order", udf_is_order(orderStream.type)) \
       .withColumn("is_return", udf_is_return(orderStream.type))



# Console Output of Orders Stream
orderStreamConsole = expandedOrderStream \
       .select("invoice_no", 
               "country", 
               "timestamp", 
               "total_cost", 
               "total_items", 
               "is_order", 
               "is_return") \
       .writeStream \
       .outputMode("append") \
       .format("console") \
       .option("truncate", "false") \
       .trigger(processingTime="1 minute") \
       .start()
       



# Calculate time based KPI and save in json format to HDFS
aggStreamByTime = expandedOrderStream \
    .withWatermark("timestamp","10 minute") \
    .groupBy(window("timestamp", windowDuration="1 minute", slideDuration="1 minute")) \
    .agg(sum("total_cost"),
         avg("total_cost"),
         count("invoice_no").alias("OPM"),
         avg("is_return"))\
    .select("window",
            "OPM",
            format_number("sum(total_cost)",2).alias("total_sale_volume"),
            format_number("avg(total_cost)",2).alias("average_transaction_size"),
            format_number("avg(is_return)",2).alias("rate_of_return")) \
    .writeStream \
    .outputMode("append") \
    .format("json") \
    .option("format","append") \
    .option("truncate", "false") \
    .option("path","Timebased-KPI/time_kpi/") \
    .option("checkpointLocation","Timebased-KPI/time_kpi/time_kpi_v1/") \
    .trigger(processingTime="1 minutes") \
    .start()




#Calculate time and country based KPI and save in json format to HDFS
aggStreamByTimeCountry = expandedOrderStream \
    .withWatermark("timestamp","10 minute") \
    .groupBy(window("timestamp", windowDuration="1 minute", slideDuration="1 minute"),"country") \
    .agg(sum("total_cost"),
         count("invoice_no").alias("OPM"),
         avg("is_return")) \
    .select("window",
            "country",
            "OPM",
            format_number("sum(total_cost)",2).alias("total_sale_volume"),
            format_number("avg(is_return)",2).alias("rate_of_return")) \
    .writeStream \
    .outputMode("append") \
    .format("json") \
    .option("format","append") \
    .option("truncate", "false") \
    .option("path","Country-and-timebased-KPI/country_kpi/") \
    .option("checkpointLocation","Country-and-timebased-KPI/country_kpi/country_kpi_v1/") \
    .trigger(processingTime="1 minute") \
    .start()
    
    


# stream termination command
orderStreamConsole.awaitTermination()
aggStreamByTime.awaitTermination()
aggStreamByTimeCountry.awaitTermination()