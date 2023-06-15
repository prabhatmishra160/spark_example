import org.apache.spark.SparkContext
import scala.io.Source

object camaign extends App {
  //campaigndata
System.setProperty("hadoop.home.dir","C:/Hadoop")


// this will run locally

 


val sc = new SparkContext("local[*]","wordcount")


val rdd1 = sc.textFile("C:/Users/prabh/Downloads/sdata/campaigndata.csv")

val rdd2=rdd1.map(x=>(x.split(",")(10).toFloat,x.split(",")(0)))

val rdd3=rdd2.flatMapValues(x=>x.split(" "))

val rdd4=rdd3.map(x=>(x._2.toLowerCase(),x._1))

val rdd5=rdd4.reduceByKey((x,y)=>x+y).sortBy(x=>x._2,false)

rdd5.collect.foreach(println)


}