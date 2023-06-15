import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.types.DateType

import org.apache.spark.sql.functions._

object BROADCASTjOIN extends App {
  
  val sparkConf=new SparkConf() 
sparkConf.set("spark.app.name","first_application")
sparkConf.set("spark.master","local[2]")

val spark = SparkSession.builder().config(sparkConf).getOrCreate() 

spark.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")
  
 val Ordrsinvoice = spark.read.format("csv").option("header",true).
  option("inferSchema",true).option("path" , "C:/Users/prabh/Downloads/sdata/ordersjoinsame.csv").load()
  


   val df2 = spark.read.format("csv").option("header",true).
  option("inferSchema",true).option("path" , "C:/Users/prabh/Downloads/sdata/customersjoin.csv").load()
  
     val join_conditio = Ordrsinvoice.col("customer_id") === df2.col("customer_id")
  val join_typ ="inner"
  val df4 = Ordrsinvoice.join(broadcast(df2),join_conditio,join_typ)
  .drop(Ordrsinvoice.col("customer_id")).select("customer_id","order_id").sort("order_id").show()
  
  
  
 scala.io.StdIn.readLine() 
  spark.stop()
}