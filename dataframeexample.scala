import org.apache.spark.sql.SparkSession


object dataframeexample extends App {
  
 System.setProperty("hadoop.home.dir","C:/Hadoop")
 //val spark = SparkSession.builder().appName("my app 1").master("local[2]").getOrCreate()
  
val spark = SparkSession.builder().appName("my application 1").master("local[2]").getOrCreate()
  println("*********************-----------**********")
  
 // val Datadf=spark.read.csv("C:/Users/prabh/Downloads/sdata/orders.csv")
  val Datadf=spark.read.option("header",true).
  option("inferSchema",true).
  csv("C:/Users/prabh/Downloads/sdata/orders.csv")
  // read is action here (1job)
  // option is also action where we have written referSchema(1 job)
 val grouporderdf = Datadf.repartition(4).where("order_customer_id > 100").select("order_customer_id,order_id").
  groupBy("order_customer_id").count()
  
  Datadf.show()// action 1 job
  Datadf.printSchema()
  
  
  
  spark.stop()
  
}