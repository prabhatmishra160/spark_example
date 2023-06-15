import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.types.DateType

import org.apache.spark.sql.functions._





object week12_session15 extends App {
  /*
   *I want to create a scala list
   * 
   * From the scala list , i want to create a dataframe with column 
   * 
   * orderid,orderdate,customerid,status
   * 
   * i want to convert Orderdate field to epoch timestamp(unix )->number of seconds after 21 jan 1970
   * create a new column with the name "newid" make sure it has unique id
   * 
   * Drop Duplicate based on column orderdate,customerid column combination session15
   * 
   * i want to drop the column orderid
   * 
   * if i want to add new column or to change the content of coluumn we use .withColumns
   * 
   * 
   *  */
  
    val sparkConf=new SparkConf() 
sparkConf.set("spark.app.name","first_application")
sparkConf.set("spark.master","local[2]")

val spark = SparkSession.builder().config(sparkConf).getOrCreate() 


val myList = List(
    (1,"2013-07-25",11599,"CLOSED"),
(2,"2017-07-25",125,"Pending"),
(3,"2016-07-25",199,"Complete"),
(4,"2014-07-25",1199,"CLOSed"),
(5,"2016-07-25",11599,"Pending")
    )
    
// val rdd = spark.sparkContext.parallelize(myList)
 
// import spark.implicits._
 
// val df1 = rdd.toDF()
    
  val ordersdf = spark.createDataFrame(myList)
  .toDF("orderid","orderdate","customerid","status")
  
  
 val df2 = ordersdf.withColumn("orderdate", unix_timestamp(col("orderdate").cast(DateType)))
 .withColumn("newid",monotonically_increasing_id)
 .dropDuplicates("customerid")
 .drop("orderid").sort("orderdate")
  
  df2.printSchema()
  df2.show()
    
    
    
    
    
    


spark.stop()
   
  
}