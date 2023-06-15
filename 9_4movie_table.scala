import org.apache.spark.SparkContext


object movie_table extends App {
System.setProperty("hadoop.home.dir","C:/Hadoop")
 
val sc = new SparkContext("local[*]","wordcount")

val rdd1 = sc.textFile("C:/Users/prabh/Downloads/sdata/moviedata.data")

val rdd2 = rdd1.map(x=>x.split("\t")(2))

val rdd3 = rdd2.map(x=>(x,1)).reduceByKey(_+_).sortBy(_._1)
rdd3.collect().foreach(println)
  
val rdd4 = rdd2.countByValue


rdd4.foreach(println)

}