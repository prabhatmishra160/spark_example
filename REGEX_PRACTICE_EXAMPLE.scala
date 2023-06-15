import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp


object REGEX_PRACTICE_EXAMPLE extends App{
   System.setProperty("hadoop.home.dir","C:/Hadoop")
  
 val myregex = """^(\S+) (\S+)\t(\S+)\,(\S+)""".r
 
 case class Orders(order_id:Int,date:String,customer_id:Int,order_status:String) 
 
 def parser(line:String)={
   line match{
     case myregex(order_id,date,customer_id,order_status) =>
       Orders(order_id.toInt,date,customer_id.toInt,order_status)
   }
   
 }
 
val sparkConf=new SparkConf() 
sparkConf.set("spark.app.name","first application")
sparkConf.set("spark.master","local[2]")

 // sparkConf.set("spark.sql.warehouse.dir", "file:///C:/Users/prabh/Desktop")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate() 
 
  
 val rdd1 = spark.sparkContext.textFile("C:/Users/prabh/Downloads/sdata/REGEX_PRACTICE.txt")
 
 import spark.implicits._
  val rdd2= rdd1.map(parser).toDS()
  rdd2.printSchema()
  //rdd2.select("order_id").show()
  
  rdd2.groupBy("order_status").count().show()
  spark.stop()
  
}