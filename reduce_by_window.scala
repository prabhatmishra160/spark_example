import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object reduce_by_window extends App {
 
   Logger.getLogger("org").setLevel(Level.ERROR)
   val sc = new SparkContext("local[*]","wordcount")
   val ssc = new StreamingContext(sc, Seconds(2))
    
    ssc.checkpoint("C:/Users/prabh/Desktop/prabhat")
    
    
   // checkpoint-> use to store rdd to remember previous state
   
   val lines = ssc.socketTextStream("localhost",9990)
   
 
   
  // val words = lines.flatMap(x=> x.split(" "))
   
   // val pairs = words.map(x=>(x,1))

   
   val word_count = lines.reduceByWindow((x : String,y : String)=>(x.toInt + y.toInt).toString(),(x : String,y : String)=>(x.toInt - y.toInt).toString(),Seconds(10),Seconds(2))
   
   // use filter to remove zero values
   
   word_count.print()
   
   ssc.start()
   
   ssc.awaitTermination()
}