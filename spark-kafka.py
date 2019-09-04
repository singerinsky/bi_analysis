from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

offsets = []

def out_put(m):
    print(m)

def save_to_mysql(output,epoch_id):
    output.write.mode("append").jdbc(url="jdbc:mysql://localhost:3306/spark",table="online",properties={"user":"root","password":"12345678","driver":"com.mysql.jdbc.Driver"})


def format_data(log_time):
    print "=",log_time,"="
    timeStruct = time.strptime(log_time,"%Y-%m-%d %H:%M:%S")
    strTime = time.strftime("%Y-%m-%d",timeStruct)
    return strTime 


if __name__ == "__main__":

    schema = StructType()\
	.add("logtype",StringType())\
	.add("name",StringType())\
	.add("log",StringType())\
	.add("log_time",StringType())

    spark = SparkSession.builder.appName("Test1").getOrCreate()

    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers","127.0.0.1:9092").option("startingOffsets","earliest").option("subscribe","bidata").load().select(from_json(col("value").cast("string"),schema).alias("result")).select("result.*")
    #count = df.select(col("logtype").alias("LogType"),col("name"),col("log"),col("log_time")).withColumn("pro_dt",format_data(col("log_time"))).where(col("logtype")=="23")

    func_data_format = udf(format_data)
    count = df.select(col("logtype").alias("LogType"),col("name"),col("log"),col("log_time"),func_data_format(col("log_time")).alias("pro_dt")).where(col("logtype")=="23")

    query=count.writeStream.outputMode("append").trigger(processingTime="1 seconds").option("checkpointLocation","checkpoint")\
	.foreachBatch(save_to_mysql).start()
    #query=count.writeStream.outputMode("append").trigger(processingTime="5 seconds").option("checkpointLocation","checkpoint").format("console").start()
    query.awaitTermination()

    
