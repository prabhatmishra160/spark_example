import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext


object statful_window_stream extends App {
  
  
   Logger.getLogger("org").setLevel(Level.ERROR)
   val sc = new SparkContext("local[*]","wordcount")
   val ssc = new StreamingContext(sc, Seconds(2))
    
    ssc.checkpoint("C:/Users/prabh/Desktop/prabhat")
    
    
   // checkpoint-> use to store rdd to remember previous state
   
   val lines = ssc.socketTextStream("localhost",9999)
   
 
   
   val words = lines.flatMap(x=> x.split(" "))
   
   val pairs = words.map(x=>(x,1))

   
   val word_count = pairs.reduceByKeyAndWindow((x,y)=>x+y,(x,y)=>x-y,Seconds(10),Seconds(2))
   .filter(x=>x._2>0)
   // use filter to remove zero values
   
   word_count.print()
   
   ssc.start()
   
   ssc.awaitTermination()
  
}