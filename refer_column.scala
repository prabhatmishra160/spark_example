import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import org.apache.spark.sql.functions._
object refer_column extends App {
 // orderss
  
  val sparkConf=new SparkConf() 
sparkConf.set("spark.app.name","first application")
sparkConf.set("spark.master","local[2]")

 // sparkConf.set("spark.sql.warehouse.dir", "file:///C:/Users/prabh/Desktop")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate() 
 val Ordrsdff = spark.read.format("csv").option("header",true).
  option("inferSchema",true).option("path" , "C:/Users/prabh/Downloads/sdata/orderss.csv").load()
// accessing through column string
//  Ordrsdff.select("order_id","order_status").show()
 //column object
  import spark.implicits._
  
  Ordrsdff.select(column("order_id"),col("order_status"),$"order_customer_id",'order_date).show()

//column Expression
  // Ordrsdff.select("order_id","concat(order_status,'_status')").show()

      /* 
        we cant mix column string with column expression nor we can mix column object with column expression
        */
      Ordrsdff.select(column("order_id"),expr("concat(order_status,'_status')")).show(false)
      // without column object shortcut
      // expr("concat(order_status,'_status')") this is column object so you need to add column with wrapper 
      // of column object
      Ordrsdff.selectExpr("order_id","concat(order_status,'_status')").show(false)
      
      
}