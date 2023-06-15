import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object spark_sql extends App {
   val sparkConf=new SparkConf() 
 sparkConf.set("spark.app.name","first application")
 sparkConf.set("spark.master","local[2]")
 
   val spark = SparkSession.builder().config(sparkConf).getOrCreate() 
 
    val Ordrsdff = spark.read.format("csv").option("header",true).
  option("inferSchema",true).option("path","C:/Users/prabh/Downloads/sdata/orders.csv").load()
  
  Ordrsdff.createOrReplaceTempView("orders")
  
  //createOrReplaceTempView will be able to use data frame Ordrsdff as a table "orders".
  // this table is distributed like dataframe
  val result_df=spark.sql("select order_status,count(*) as status_count from orders group by order_status")
  result_df.show()
  // spark.sql function gives dataframe as a return 
  
  // performance of sql table equal to data frame
  
  
  val result_data=spark.sql("select order_customer_id,count(*) as total_orders from orders where "+
 "order_status='CLOSED' group by order_customer_id order by total_orders desc ")
 
 result_data.show()
 spark.stop()
}