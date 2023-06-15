
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object spark_stream_less_partiotion extends App  {
   /*
   * Generate some words throgh a netcat Utility
   * nc -lk portnumber  or ncat -lk portnumber
   * 
   * spark structured streaming code which reads from the above socket 
   * and calculatrs the frequency of each word
   * 
   * 
spark.conf.set("spark.sql.shuffle.partitions",100)
println(df.groupBy("_c0").count().rdd.partitions.length)
   * 
   */
  
  
  
  //Logger.getLogger("org").setLevel(Level.ERROR)
  


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




// 3. write to the sink

val word_count = countdf.writeStream.format("console").outputMode("complete")
.option("checkpointLocation","C:/Users/prabh/Desktop/prabhat").start()

    

 
   
   word_count.awaitTermination()
  
}
  
  




