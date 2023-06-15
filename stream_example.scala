import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Level
import org.apache.log4j.Logger


object stream_example extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
   val sc = new SparkContext("local[*]","wordcount")
   val ssc = new StreamingContext(sc, Seconds(5))
   
   val lines = ssc.socketTextStream("localhost",9995)
   
   val words = lines.flatMap(x=> x.split(" "))
   
   val pairs = words.map(x=>(x,1))
   
   val word_count = pairs.reduceByKey((x,y)=>x+y)
   
   word_count.print()
   
   ssc.start()
   
   ssc.awaitTermination()
   
}