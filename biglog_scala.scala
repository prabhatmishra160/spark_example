import org.apache.spark.SparkContext


object biglog_scala extends App {
    System.setProperty("hadoop.home.dir","C:/Hadoop")
  val sc = new SparkContext("local[*]","wordcount")
  
  val rdd1 = sc.textFile("C:/Users/prabh/Downloads/sdata/bigLog.txt")
     val pairrdd=rdd1.map(x=>{
     
     val columns =x.split(":")
    
     (columns(0),columns(1))
          
   })
 pairrdd.groupByKey.collect().foreach(x=>println(x._1,x._2.size))
   

   
}