

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import java.sql.Timestamp


object sparkReaddata extends App {
  
val sparkconf = new SparkConf()
sparkconf.set("spark.app.name","my first app")
sparkconf.set("spark.master","local[2]")
val spark = SparkSession.builder().config(sparkconf).getOrCreate()
//val Ordrsdf = spark.read.option("header",true).format("csv").
//  option("inferSchema",true).option("path" , "C:/Users/prabh/Downloads/sdata/orders.csv")
// .load()

/*
val Ordrsdf = spark.read.format("json").option("path" , "C:/Users/prabh/Downloads/sdata/players.json")
.option("mode","FAILFAST").load()
Ordrsdf.show() */
val Ordrsdf = spark.read.option("path" , "C:/Users/prabh/Downloads/sdata/users.parquet")
.load()
Ordrsdf.show(false)
//.format("parquet") // by default format is parqut so no need to write
// 3 read mode 1.permissive ->default(null corrupt record),Dropmalformed(ignored),Failfast(exception)

// in json its infer schema some extent

spark.close()


}