
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.types.DateType

import org.apache.spark.sql.functions._

object aggregate_transformation extends App {
  // ordersssss_data.csv
  /*
   * load the file and create a data frame using satanderd data frame reader api
   * 
   * simple aggregate ->total number of rows,total quantities,avg unit price,number of uniqueinvoices
   * calculate this using column object expression
   * calculate this using string expression
   * calculate this using spark SQL
   * 
   */
  
      val sparkConf=new SparkConf() 
sparkConf.set("spark.app.name","first_application")
sparkConf.set("spark.master","local[2]")

val spark = SparkSession.builder().config(sparkConf).getOrCreate() 
  
 val Ordrsinvoice = spark.read.format("csv").option("header",true).
  option("inferSchema",true).option("path" , "C:/Users/prabh/Downloads/sdata/ordersssss_data.csv").load()
  
  //*********** column object expression ***************
  
 val df = Ordrsinvoice.select(
    count("*").as("row_count"),
    sum("Quantity").as("total Quantity"),
    countDistinct("InvoiceNo").as("count Distict"),
    avg("UnitPrice").as("avgPrice")
  
  ).show()
  
  
  
  
    //*********** string expression ***************
    //*********** column object expression ***************
  Ordrsinvoice.selectExpr(
     "count(*) as row_count",
     "sum(Quantity) as total_Quantity",
"count(Distinct(InvoiceNo)) as count_Distict",
" avg(UnitPrice) as avgPrice"
     ).show()
     
     
     
       //*********** sql expression ***************
     
     
   Ordrsinvoice.createOrReplaceTempView("sales")  
   
   spark.sql("select count(*),sum(Quantity),count(Distinct(InvoiceNo)) from sales").show()
     
     
     
  
  spark.stop()
  
}