import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

object watermark_streaming extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  
val spark = SparkSession.builder().master("local[2]").appName("myapp")
.config("spark.sql.shuffle.partitions",3)
.getOrCreate()
  // Define schema
  val order_schema = StructType(List(
  StructField("order_id",IntegerType),    
  StructField("order_date",StringType),  
  StructField("order_customer_id",IntegerType),  
  StructField("order_status",StringType),
  StructField("amount",IntegerType)
  ))

//1.Read  the data stream from socket

 val linesDf = spark.readStream.format("socket").option("host","localhost")
  .option("port","9911").load()
  
  // what ever you will read from json that will come as a value type single column
  
/*val valuedf = linesDf.select(from_json(col("value"), order_schema).alias("value"))
  *
  * will come value column everything
  */
val valuedf = linesDf.select(from_json(col("value"), order_schema).alias("value"))

val refineorderdf = valuedf.select("value.*")

 
/*
  val windowagg =
 refineorderdf
 .groupBy(window(col("order_date"),"15 minute"))
 .agg(sum("amount").alias("totalinvoice"))
 
 windowagg .printSchema()
 * will again give two level value under window
 */
 
  val windowagg = refineorderdf
  .withWatermark("order_date", "30 minute")
 .groupBy(window(col("order_date"),"15 minute"))
 .agg(sum("amount").alias("totalinvoice"))
 
 val outputdf = windowagg .select("window.start","window.end","totalinvoice")
 
 // or  val outputdf = windowagg .select("window.*")
 
 
 // write to sink
 val order_query =
 outputdf.writeStream.format("console").outputMode("update")
.option("checkpointLocation","C:/Users/prabh/Desktop/prabhat6	")
.trigger(Trigger.ProcessingTime("15 seconds"))
.start()

   // recorrd will process on event time not trigger time 

 
   
  order_query.awaitTermination()
 
 
 
 
 
  
}