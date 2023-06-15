import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Level
import org.apache.log4j.Logger

object statfull_streaming extends App {
  
    Logger.getLogger("org").setLevel(Level.ERROR)
   val sc = new SparkContext("local[*]","wordcount")
   val ssc = new StreamingContext(sc, Seconds(5))
    
    ssc.checkpoint("C:/Users/prabh/Desktop/prabhat")
    
    
   // checkpoint-> use to store rdd to remember previous state
   
   val lines = ssc.socketTextStream("localhost",9996)
   
   def updatefunc(newValues:Seq[Int] , previousstat : Option[Int]):Option[Int] ={
   val newcount =  previousstat.getOrElse(0) + newValues.sum
   
   Some(newcount)
   
   /*
    * getOrElse->option type that check if prvious value exist it ill return that or else return given value in parameter
    * 
    * 
    * Some-> return value
    */
   }
   
   val words = lines.flatMap(x=> x.split(" "))
   
   val pairs = words.map(x=>(x,1))
   
   /*
    * updateStateByKey(func)=>
    * this require a 2 steps ->1.define a start to start with ,,,,2->define a function to update state
    * func->is fi=unction here to update state
    * func need to parameter -> 1--->new value that we want to add
    *                           2--->previous state
    */
   
   val word_count = pairs.updateStateByKey(updatefunc)
   
   word_count.print()
   
   ssc.start()
   
   ssc.awaitTermination()
}