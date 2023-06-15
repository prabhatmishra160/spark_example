import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger


object SecondMethodForDatarame extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  //i want to see only error messages
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","first application")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
  val Ordrsdf=spark.read.option("header",true).
  option("inferSchema",true).
  csv("C:/Users/prabh/Downloads/sdata/orders.csv")
  val groupOrder=Ordrsdf.repartition(4).where("order_customer_id>1000").select("order_id","order_customer_id").
  groupBy("order_customer_id").count()
  
  // everything above here is transformation
  // when ever internal stages create it writes rdd into internal buffer called stages
  groupOrder.show()
  
  // for own log message
  Logger.getLogger(getClass.getName).info("my application successfully completed")
  spark.close()
}