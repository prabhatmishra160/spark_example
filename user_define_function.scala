import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//.udf


object user_define_function extends App {
 //System.setProperty("hadoop.home.dir","C:/Hadoop")
def idCheck(id:String):String={
      var ids =id.toInt
  if (ids>10) "Y" else "N"
  
  
}  
  
  case class customer(customer_id:String,customer_fname:String,customer_lname: String ,
      customer_email:String,customer_password:String,customer_city:String,
      customer_state:String,customer_zipcode:String)
  val sparkConf=new SparkConf() 
sparkConf.set("spark.app.name","first_application")
sparkConf.set("spark.master","local[2]")

 // sparkConf.set("spark.sql.warehouse.dir", "file:///C:/Users/prabh/Desktop")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate() 
 /*val Ordrsdff = spark.read.format("csv").
  option("inferSchema",true).option("path","C:/Users/prabh/Downloads/sdata/customersss").load()*/
  // val Ordrsd = Ordrsdff.toDF("name","age","city")
  
val df = spark.read.option("header", true).csv("C:/Users/prabh/Downloads/sdata/customersss.csv")
    df.printSchema()
    
import spark.implicits._
    
  val ds1=df.as[customer]
  ds1.filter(x=>x.customer_id.toInt>=10).show()
  
//val parse_id= udf(idCheck(_:String):String)==>this way it will not register in spark catalog
//val df2=df.withColumn("above_id_10",parse_id(col("customer_id")))
//df2.show(false)
  // spark.catalog.listFunctions().filter(x=>x.name=="parse_id")

  spark.udf.register("parse_id", idCheck(_:String):String)
  
 df.withColumn("adult",expr("parse_id('customer_id')")).show()
  
  

df.show(false)
  spark.stop()
}
