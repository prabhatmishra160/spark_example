

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger


object streaming_readData_from_source extends App  {
  
  
 Logger.getLogger("org").setLevel(Level.ERROR)

  
val spark = SparkSession.builder().master("local[2]").appName("myapp")
.config("spark.sql.shuffle.partitions",3).config("spark.sql.streaming.schemaInference","true")
.getOrCreate()
 // sparkConf.set("spark.sql.warehouse.dir", "file:///C:/Users/prabh/Desktop")

/*
 * spark straming offer 4 built in data source
 * 
 * 1-> socket(ip+port number) source =>
 * 
 * not production level
 * 
 * 2.Rate source =>
 * 
 * used for testing and bench mark purpose
 * 
 * 3 . File source =>
 * 
 * 
 * Kafka
 * 
 */

//1.Read from the stream

// val linesDf = spark.readStream.format("socket").option("host","localhost")
//  .option("port","9974").load()
// read data from file source

// linesDf.printSchema()

val orderDf = spark.readStream.format("json").option("path","C:/Users/prabh/Downloads/stream")
  .load()

// .option("maxFilesPerTrigger",1) => to process no of file in one trigger(batch)


//2. Processes

  
orderDf.createOrReplaceTempView("orders")
val read_order = spark.sql("select * from orders where order_status = 'COMPLETE'")


    
val word_count = read_order.writeStream.format("json").outputMode("append")
.option("path","C:/Users/prabh/Downloads/stream/pk")
.option("checkpointLocation","C:/Users/prabh/Desktop/prabhat3")
.trigger(Trigger.ProcessingTime("30 seconds"))
.start()

    

 
   
   word_count.awaitTermination()
  
  
}