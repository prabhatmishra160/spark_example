
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object count_by_window extends App {
  // Count the number of line in the window
  
  
   Logger.getLogger("org").setLevel(Level.ERROR)
   val sc = new SparkContext("local[*]","wordcount")
   val ssc = new StreamingContext(sc, Seconds(2))
    
    ssc.checkpoint("C:/Users/prabh/Desktop/prabhat")
    
    
   // checkpoint-> use to store rdd to remember previous state
   
   val lines = ssc.socketTextStream("localhost",9980)
   
 
   
   val words = lines.countByWindow(Seconds(10),Seconds(2))
   
 
   // use filter to remove zero values
   
   words.print()
   
   ssc.start()
   
   ssc.awaitTermination()
  
}