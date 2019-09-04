from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import *
from pyspark.sql.types import *

offsets = []

def out_put(m):
    print(m)

def store_offset(rdd):
    global offsets
    offsets = rdd.offsetRanges()
    return rdd

def print_offset(rdd):
    for o in offsets:
        print "%s %s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset, o.untilOffset - o.fromOffset)


if __name__ == "__main__":

    schema = StructType()\
	.add("logtype",StringType())\
	.add("name",StringType())\
	.add("log",StringType())

    spark = SparkSession.builder.appName("Test1").getOrCreate()

    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers","127.0.0.1:9092").option("startingOffsets","earliest").option("subscribe","bidata").load().select(from_json(col("value").cast("string"),schema).alias("result")).select("result.*")
    count = df.select(col("logtype").alias("LogType"),col("name"),col("log"))

    query=count.writeStream.outputMode("append").trigger(processingTime="5 seconds").option("checkpointLocation","checkpoint").format("console").start()
    query.awaitTermination()

    
