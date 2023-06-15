import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.types.DateType

import org.apache.spark.sql.functions._

object Grouping_aggregation extends App {
val sparkConf=new SparkConf() 
sparkConf.set("spark.app.name","first_application")
sparkConf.set("spark.master","local[2]")

val spark = SparkSession.builder().config(sparkConf).getOrCreate() 
  
 val Ordrsinvoice = spark.read.format("csv").option("header",true).
  option("inferSchema",true).option("path" , "C:/Users/prabh/Downloads/sdata/ordersssss_data.csv").load()
  
  /*
   * we want to group the data based on country and invoce number
   * 
   * i want total quantity for each group also we want sum of invoice value
   *    * calculate this using column object expression
   * calculate this using string expression
   * calculate this using spark SQL
   */
  
    //*********** column object expression ***************
  
 val summarydf = Ordrsinvoice.groupBy("Country","InvoiceNo")
  .agg(
  sum("Quantity").as("total Quantity"),
  sum(expr("Quantity * UnitPrice")).as("invoice value")
  )
  
  summarydf.show()
  
  
   //*********** string expression ***************
 val df1 = Ordrsinvoice.groupBy("Country","InvoiceNo")
 .agg(expr("sum(Quantity) as total_Quantity"),
     expr("sum(Quantity * UnitPrice) as invoice_value")
     
 )
  
  df1.show()
  
  
  
  
         //*********** sql expression ***************
     
     
   Ordrsinvoice.createOrReplaceTempView("sales")  
   
   spark.sql("select Country,InvoiceNo,sum(Quantity) as total_Quantity,sum(Quantity * UnitPrice) as invoice_value from sales group by Country,InvoiceNo").show()
     
     
  
  
  
  spark.stop()
}