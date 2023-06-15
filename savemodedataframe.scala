// ***********week 12 1st video****************
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import java.sql.Timestamp
import org.apache.spark.sql.SaveMode

object savemodedataframe extends App{
   val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","first application")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
  val Ordrsinvoice = spark.read.format("csv").option("header",true).
  option("inferSchema",true).option("path" , "C:/Users/prabh/Downloads/sdata/ordersssss_data.csv").load()

  /*
   *   val df = Ordrsinvoice.write
  .format("csv")
  .mode(SaveMode.Overwrite)
  .option("path" , "C:/Users/prabh/Downloads/sdata/ordersssfolder").save()
   */
    
  
  //output is directory
  val df = Ordrsinvoice.write
  .format("avro").partitionBy("Country")
  .mode(SaveMode.Overwrite)
  .option("path" , "C:/Users/prabh/Downloads/sdata/ordersssfolder").save()
  
  
  
  /*
   *   val df = Ordrsinvoice.write
  .format("csv").partitionBy("Country")
  .option("maxRecordPerFile",2000)
  .mode(SaveMode.Overwrite)
  .option("path" , "C:/Users/prabh/Downloads/sdata/ordersssfolder").save()
   */
  
  spark.stop()
}