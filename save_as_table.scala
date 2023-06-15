import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object save_as_table extends App {
   val sparkConf=new SparkConf() 
 sparkConf.set("spark.app.name","first application")
 sparkConf.set("spark.master","local[2]")

 // sparkConf.set("spark.sql.warehouse.dir", "file:///C:/Users/prabh/Desktop")->admin will decide
 // enableHiveSupport to store meta data in hive meta store
  val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate() 
 // System.setProperty("hadoop.home.dir","C:/Hadoop")
  // spark hive spark version scala version
  
   val Ordrsdff = spark.read.format("csv").option("header",true).
  option("inferSchema",true).option("path" , "C:/Users/prabh/Downloads/sdata/orders.csv").load()
  Ordrsdff.show()
  
  // saveAsTable to store tjhe data as a table
 // Ordrsdff.write.format("csv").mode(SaveMode.Overwrite).saveAsTable("ordersaasd")
  // by default ordersaasd create in default database (spark warehouse)
  // but meta store is in memory meta store so once you stop application it will go away
  // to keep meta store persistant store it in hive metastore for that you need to add spark hive jar 
  // following spark version with scala version
 /*
  spark.sql("create database if not exists retail")
  Ordrsdff.show()
  Ordrsdff.write.format("csv").mode(SaveMode.Overwrite).saveAsTable("retail.orders")
  Ordrsdff.show()
  * 
  */
 
  // bucketBy work only when we say saveAsTable
    spark.sql("create database if not exists retail")
  Ordrsdff.show()
  Ordrsdff.write.format("csv").mode(SaveMode.Overwrite)
  .bucketBy(4,"order_id").sortBy("order_id")
  .saveAsTable("retail.orders")
  Ordrsdff.show()
   
  
  spark.catalog.listTables("retail").show() // it will show all table in retail database

spark.stop()
}