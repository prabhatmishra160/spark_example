import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode


object save_file extends App {
 val sparkConf=new SparkConf() 
 sparkConf.set("spark.app.name","first application")
 sparkConf.set("spark.master","local[2]")

 // sparkConf.set("spark.sql.warehouse.dir", "file:///C:/Users/prabh/Desktop")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate() 
 // System.setProperty("hadoop.home.dir","C:/Hadoop")
  
   val Ordrsdff = spark.read.format("csv").option("header",true).
  option("inferSchema",true).option("path" , "C:/Users/prabh/Downloads/sdata/orders.csv").load()
  
// Ordrsdff.write.format("csv").partitionBy("order_status").mode(SaveMode.Overwrite).option("path","C:/Users/prabh/Desktop/spark_output").save()
// By default format is parquet
  // getNumPartion not available for data frame
  /*
   * Ordrsdff.rdd.getNumPartitions()
   * val df = Ordrsdff.repartitions(4)
   *  df.rdd.getNumPartitions()
   * /
   * n0 of file =number of partition
   */
  // append
  //, overwrite,errorIfExists,ignore(save Mode)
  Ordrsdff.write.format("csv").partitionBy("order_status").
  mode(SaveMode.Overwrite).option("maxRecordsPerFile",2000)
  .option("path","C:/Users/prabh/Desktop/spark_output").save()
 //option("maxRecordsPerFile",n)->not to more than n no of  of records
  // format "avro" is not supported by default for that i need to download jar and add a jar
  
spark.stop()
}