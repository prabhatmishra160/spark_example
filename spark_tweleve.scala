import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level


object spark_tweleve extends App {
  
// System.setProperty("hadoop.home.dir","C:/Hadoop")
 
   /*
  * i will load this file as a rdd
  * each line of the rdd is of string type
  * use a map transformation which is low level transformation
  * input to the map transformation  i will use regular expression
  * if we have schema associated /structure associated we can convert our rdd to a dataset
  * 
  * then i will associate the out put with the case class so that we have structured associated with it.
  * on
  */
val sparkConf=new SparkConf() 
sparkConf.set("spark.app.name","fst_application")
sparkConf.set("spark.master","local[2]")

 // sparkConf.set("spark.sql.warehouse.dir", "file:///C:/Users/prabh/Desktop")
  
val spark = SparkSession.builder().config(sparkConf).getOrCreate() 
   val myregex = """^(\S+) (\S+)\t(\S+)\,(\S+)""".r
   
    case class Orders(order_id:Int,date:String,customer_id:Int,order_status:String)
 
    def parser(line:String)={
   line match{
     case myregex(order_id,date,customer_id,order_status) => Orders(order_id.toInt,date,customer_id.toInt,order_status)
      case _      => Orders(0,"0",0,"0")
   }
   }
  
 val lines = spark.sparkContext.textFile("C:/Users/prabh/Downloads/sdata/REGEX_PRACTICE.txt")

 import spark.implicits._

 val ordersDs = lines.map(parser).toDS()

 ordersDs.select("order_id").show()
 
 ordersDs.groupBy("order_status").count().show()
 
spark.stop()
 
}