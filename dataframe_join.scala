import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.types.DateType

object dataframe_join extends App {
val sparkConf=new SparkConf() 
sparkConf.set("spark.app.name","first_application")
sparkConf.set("spark.master","local[2]")

val spark = SparkSession.builder().config(sparkConf).getOrCreate() 
  
 val Ordrsinvoice = spark.read.format("csv").option("header",true).
  option("inferSchema",true).option("path" , "C:/Users/prabh/Downloads/sdata/ordersjoin.csv").load()
  
  /*
   * we have two data set 
   * orders and customer
   * order_customer_id,customer_id two column on which we will perform join operations customersjoin
   inner_join->matching record from both table
   in this case we wont see the customer who never placed a order
   
   outer->matching records + non matching records from the left table + non matching records from the right table
   
   Left-> >matching records + non matching records from the left table
   
   right -> matching records + non matching records from the right table
   */

   val df2 = spark.read.format("csv").option("header",true).
  option("inferSchema",true).option("path" , "C:/Users/prabh/Downloads/sdata/customersjoin.csv").load()
  
 // val df3 = Ordrsinvoice.join(df2,Ordrsinvoice.col("order_customer_id") === df2.col("customer_id"),"inner")
// df3.show() 
  val join_condition = Ordrsinvoice.col("order_customer_id") === df2.col("customer_id")
  val join_type ="inner"
  val df3 = Ordrsinvoice.join(df2,join_condition,join_type)
 df3.show() 
  
  // Lets a some customer never place order but we dont want to miss them
 
  val join_conditio = Ordrsinvoice.col("order_customer_id") === df2.col("customer_id")
  val join_typ ="right"
  val df4 = Ordrsinvoice.join(df2,join_conditio,join_typ).sort("customer_id").show()
 
spark.stop()
}