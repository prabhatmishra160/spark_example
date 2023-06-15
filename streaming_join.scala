
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

object streaming_join extends App {
  
    
  val sparkConf=new SparkConf() 
  val spark = SparkSession.builder().master("local[2]").appName("myapp")
.config("spark.sql.shuffle.partitions",3).config("spark.sql.streaming.schemaInference","true")
.getOrCreate()

val transactionSchema = StructType(
    List(
        StructField("card_id",LongType),
        StructField("amount",IntegerType),
        StructField("postcode",IntegerType),
        StructField("post_id",LongType),
        StructField("transactiondate",TimestampType)
        ))
        
       val transactionDf = spark.readStream.format("socket").option("host","localhost")
        .option("port","9980").load()
        
      //  transactionDf.printSchema()
        
 val valueDf = transactionDf.select(from_json(col("value"),transactionSchema).alias("value"))
       valueDf.printSchema()
   
 val refinedtransaction =  valueDf.select("value.*")
     
   //  refinedtransaction.printSchema()
 //Load static dataframe
 
val memberdf = spark.read.format("csv").option("header",true)
 .option("inferSchema",true).option("path","C:/Users/prabh/Downloads/jaruri.csv").load()
 
 val joincondition = refinedtransaction.col("card_id") === memberdf.col("card_id")
 
 val joinType = "inner"
 
val enrichedDf = refinedtransaction.join(memberdf, joincondition , joinType ).drop(memberdf.col("card_id"))
//write to stream
val tranquery = enrichedDf.writeStream.format("console").outputMode("update").option("checkpointlocation", "locationaskfhsk")
.trigger(Trigger.ProcessingTime("15 second")).start()

tranquery.awaitTermination()

 
     
     
        
}



