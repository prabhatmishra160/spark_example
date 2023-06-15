import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import java.sql.Timestamp
import org.apache.spark.sql.SaveMode
//***********week 12 session 23**************************
object LogFile extends App {
  
     val sparkConf=new SparkConf() 
 sparkConf.set("spark.app.name","first application")
 sparkConf.set("spark.master","local[2]")
 
   val spark = SparkSession.builder().config(sparkConf).getOrCreate() 
   
   /*
    * Grouping based on Logging level and month
    * 
    * i have 5 diffrent logging level
    *  and i have 12 month
    *  
    *  so basically output should contains 60 rows
    *  
    */
   
     
  val Ordrsinvoice = spark.read.format("csv").option("header",true)
  .option("inferSchema",true).option("path" , "C:/Users/prabh/Downloads/sdata/Log.txt").load()
  

     
     
   Ordrsinvoice.createOrReplaceTempView("log_table")  
   
  // spark.sql("select level,collect_list(datetime) from log_table group by level order by level").show(false)
  
// val df =  spark.sql("select level,date_format(datetime,'MMM') as month ,count(1) from log_table group by level,month order by month")

//  val df =  spark.sql("select level,date_format(datetime,'MMM') as month,cast(first(date_format(datetime,'M')) as int) as month_num ,count(1) as total from log_table group by level,month order by month_num , total")

  // df.createOrReplaceTempView("result_table") 
  
 // val df2 = df.drop("month_num").show(50)
 
  
  //***************Pivot table view***********************************************************
  //group by level,month order by month_num , total")
   
   // pivot("month")->system has to internally run a query to find distinct month and show case them in column
   
   // so to optimize the query we can create list of such distinct month 
   
   // List("Apr" , "Aug" , "Dec" , "Feb" , "Jan" , "Jul" , "Jun" , "Mar" , "May" , "Nov" , "Oct" , "Sep")
   
  //  val df =  spark.sql("select level,date_format(datetime,'MMM') as month,cast(date_format(datetime,'M') as int) as month_num from log_table ").groupBy("level").pivot("month").count().show(50)

  // since we are already passing the sysytem so we are telling the system dont rum internal distinct query we are gib=ving you distinct value
 // ****************Otimize*************************************
   
   val num =List("Apr" , "Aug" , "Dec" , "Feb" , "Jan" , "Jul" , "Jun" , "Mar" , "May" , "Nov" , "Oct" , "Sep")
   
   val df =  spark.sql("select level,date_format(datetime,'MMM') as month,cast(date_format(datetime,'M') as int) as month_num from log_table ").groupBy("level").pivot("month",num).count().show(50)

  spark.stop()
}