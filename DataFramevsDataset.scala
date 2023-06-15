
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import java.sql.Timestamp

case class OrdersData(order_id:Int,order_date:Timestamp, order_customer_id : Int,order_status:String) 

object DataFramevsDataset extends App {
  //  Logger.getLogger("org").setLevel(Level.ERROR)
  //i want to see only error messages
  //val spark = SparkSession.builder().appName("my app 1").master("local[2]").getOrCreate()
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","first application")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
  val Ordrsdf: Dataset[Row] = spark.read.option("header",true).
  option("inferSchema",true).
  csv("C:/Users/prabh/Downloads/sdata/orders.csv")
  
  // spark.read.option dataframe reader which is Dataset[Row]
  Ordrsdf.filter("order_id<10").show() //will execute
  
  //Ordrsdf.filter("order_ids<10").show() // will show run time error because of wrong column name and its dataframe
  import spark.implicits._ 
  // you can import this only after spark session spark is that seesion in spark.implicits
 val orderDS = Ordrsdf.as[OrdersData]
 orderDS.filter(x=>x.order_id<10).show()
}
