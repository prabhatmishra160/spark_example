import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import java.sql.Timestamp




object eplicitschema extends App {
  
  case class Ordersss(orderid:Int,orderdate:Timestamp,custid:Int,orderstatus:String)
  
 val sparkConf=new SparkConf() 
 sparkConf.set("spark.app.name","first application")
 sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
/* val order_schema=StructType(List(
 StructField("orderid",IntegerType,false),
 StructField("orderdate",TimestampType,true),
 StructField("customerid",IntegerType),
 StructField("status",StringType)
 //true->nullable
     )) 
     
 val Ordrsdff = spark.read.format("csv").option("header",true).
  schema(order_schema).option("path" , "C:/Users/prabh/Downloads/sdata/orders.csv").load()
*/
  
val ordersSChemaDDL="orderid Int,orderdate Timestamp,custid Int,orderstatus String"
 val Ordrsdff = spark.read.format("csv").option("header",true).
  schema(ordersSChemaDDL).option("path" , "C:/Users/prabh/Downloads/sdata/orders.csv").load()
import spark.implicits._
  val orerd =Ordrsdff.as[Ordersss]
 orerd.show()
Ordrsdff.show()
spark.stop()


}