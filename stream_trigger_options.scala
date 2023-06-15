

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object stream_trigger_options extends App {
Logger.getLogger("org").setLevel(Level.ERROR)

  
val spark = SparkSession.builder().master("local[2]").appName("myapp")
.config("spark.sql.shuffle.partitions",3)
.getOrCreate()
 // sparkConf.set("spark.sql.warehouse.dir", "file:///C:/Users/prabh/Desktop")


//1.Read from the stream

val linesDf = spark.readStream.format("socket").option("host","localhost")
.option("port","9974").load()

// linesDf.printSchema()


//2. Processes
  val wordsdf = linesDf.selectExpr("explode(split(value,' ')) as word")
 val countdf = wordsdf.groupBy("word").count()
  
  /*
   * splitsplit will always give you an array
   * 
   * we want to break thi array oin multiple row where each row have one words so we want 
   * to do explode on array to get each word in a different row
   *  
   * when you use select then you can just use column of dataframe
   * but when we use selectExpr then we can write sql function in thatval wordsdf 
   */


 
 /*
 * We have 4 option available for triigering streaming
 * 
 * 1->
 * unspecified->By default new create as soon as current micro batch finishes
 * 
 * 
 * 2->
 * time intervel
 * first trigger imidetely but later will trigger after time is elapsed
 * 
 * 
 * 
 * 3->
 * one time-> will triiger one tine time like batch processing but keep all the track of previous
 * 
 * processings
 * 
 * 
 * 
 * 4> continuous->experimental phase
 */

 


// 3. write to the sink

val word_count = countdf.writeStream.format("console").outputMode("complete")
.option("checkpointLocation","C:/Users/prabh/Desktop/prabhat2")
.trigger(Trigger.ProcessingTime("30 seconds"))
.start()

    

 
   
   word_count.awaitTermination()
  
  
}