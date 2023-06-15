


  


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.types.DateType

import org.apache.spark.sql.functions._

object ambiguious_null_deals extends App{
val sparkConf=new SparkConf() 
sparkConf.set("spark.app.name","first_application")
sparkConf.set("spark.master","local[2]")

val spark = SparkSession.builder().config(sparkConf).getOrCreate() 
  
 val Ordrsinvoice = spark.read.format("csv").option("header",true).
  option("inferSchema",true).option("path" , "C:/Users/prabh/Downloads/sdata/ordersjoinsame.csv").load()
  
  /*
   * we have two data set 
   * orders and customer
   *ambigous happen when we try to select a column name which is coming from two data fram
   * 
   * How to deal with this problem ->>>>>>>
   * 
   * there are two ways**********************
   * 
   * 1-> rename the ambiguous column in one of the data frame
   * .withColumnRenamed("old name","new name")
   * 
   * 2->
   * once the join done drop one of the column
   * 
   */

   val df2 = spark.read.format("csv").option("header",true).
  option("inferSchema",true).option("path" , "C:/Users/prabh/Downloads/sdata/customersjoin.csv").load()
  
 // val df3 = Ordrsinvoice.join(df2,Ordrsinvoice.col("order_customer_id") === df2.col("customer_id"),"inner")
// df3.show() 
 // val join_condition = Ordrsinvoice.col("customer_id") === df2.col("customer_id")
 // val join_type ="outer"
 // val df3 = Ordrsinvoice.join(df2,join_condition,join_type).select("customer_id","order_id").show()
  
  // will show customer_id ambigious error
  

  
 
  
   
  
  //***********************method 1*******************************
  
  /*
  val orderdf = Ordrsinvoice.withColumnRenamed("customer_id","cust_id")
  val join_condition = orderdf.col("cust_id") === df2.col("customer_id")
  val join_type ="outer"
  

 
 val df3 = orderdf.join(df2,join_condition,join_type).select("customer_id","order_id").show()
 
  */
 
  //***********************method 2(Dr0p)*******************************
 
  /*
  
   val join_conditio = Ordrsinvoice.col("customer_id") === df2.col("customer_id")
  val join_typ ="outer"
  val df4 = Ordrsinvoice.join(df2,join_conditio,join_typ)
  .drop(Ordrsinvoice.col("customer_id")).select("customer_id","order_id").sort("order_id").show()
  
  
   */
 
 // ** whenever order id is null show -1
  // in such case you can use coalesce (used to handle null)
   
   
  
 
     val join_conditio = Ordrsinvoice.col("customer_id") === df2.col("customer_id")
  val join_typ ="outer"
  val df4 = Ordrsinvoice.join(df2,join_conditio,join_typ)
  .drop(Ordrsinvoice.col("customer_id")).select("customer_id","order_id").sort("order_id")
  .withColumn("order_id",expr("coalesce(order_id,-1)"))
  .show(200)
  
  spark.stop()
  
}