import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.types.DateType

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object window_aggregation extends App {
  
  val sparkConf=new SparkConf() 
sparkConf.set("spark.app.name","first_application")
sparkConf.set("spark.master","local[2]")

val spark = SparkSession.builder().config(sparkConf).getOrCreate() 
  
 val Ordrsinvoice = spark.read.format("csv").option("header",true).
  option("inferSchema",true).option("path" , "C:/Users/prabh/Downloads/sdata/windowdata.csv").load()
  /*
   * calculate a running total of invoice value for each country
   * 
   */
val mywindow = Window.partitionBy("country").orderBy("weeknum")
.rowsBetween(Window.unboundedPreceding,Window.currentRow)

  
 val df2 = Ordrsinvoice.withColumn("runningTotal",sum("invoicevalue").over(mywindow))
  
  df2.show()
 // spark.sql("select *, over(partion by country order by weeknum ) from country_data ")
  
 // Ordrsinvoice.show()  
  
  
  spark.stop()
}