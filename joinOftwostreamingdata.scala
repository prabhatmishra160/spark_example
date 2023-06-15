import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType


object joinOftwostreamingdata extends App {
  
  val sparkConf=new SparkConf() 
  val spark = SparkSession.builder().master("local[2]").appName("myapp")
.config("spark.sql.shuffle.partitions",3).config("spark.sql.streaming.schemaInference","true")
.getOrCreate()

val impressionSchema = StructType(
    List(
        StructField("impressionid",StringType),
        StructField("impressionTime",TimestampType),
        StructField("campaignName",StringType)
        ))

        
   val clickSchema = StructType(
    List(
        StructField("clickID",StringType),
        StructField("clickTime",TimestampType)
        ))     
        
        //read the data from socket impression
        
       val impressiondf = spark.readStream.format("socket").option("host","localhost")
        .option("port","13244").load()
        
               //read the data from click impression
        
       val clickdf = spark.readStream.format("socket").option("host","localhost")
        .option("port","13245").load()
        
      //  transactionDf.printSchema()
        
        //structure the data based on the schema defined ->>>>impressiondf
        
 val valueDf1 = impressiondf.select(from_json(col("value"),impressionSchema).alias("value"))
  val refinedimpression =  valueDf1.select("value.*")
     
  
          //structure the data based on the schema defined ->>>>impressiondf
        
 val valueDf2 = clickdf.select(from_json(col("value"),clickSchema).alias("value"))
  val refinedclickdf =  valueDf2.select("value.*")
     
  
  
  

 
 val joincondition = refinedimpression.col("impressionid") === refinedclickdf.col("clickID")
 
 val joinType = "inner"
 
val enrichedDf = refinedimpression.join(refinedclickdf, joincondition , joinType ).drop(refinedclickdf.col("clickID"))
//write to stream
val tranquery = enrichedDf.writeStream.format("console").outputMode("append").option("checkpointlocation", "locationaskfhsdsdfsk")
.trigger(Trigger.ProcessingTime("15 second")).start()

tranquery.awaitTermination()

 
     
     
  
}