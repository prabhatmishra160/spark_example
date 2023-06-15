import org.apache.spark.SparkContext


object customerspent extends App {
   //local[*]=>local cluster * means use all core
  // word count=>application name
 
//Logger.getLogger("org").setLevel((Level.ERROR) 
  System.setProperty("hadoop.home.dir","C:/Hadoop")
 
val sc = new SparkContext("local[*]","wordcount")

val rdd1 = sc.textFile("C:/Users/prabh/Downloads/sdata/customerorders.csv")

val rdd2=rdd1.map(x=>(x.split(",")(0),x.split(",")(2).toFloat))
val rdd3=rdd2.reduceByKey((x,y)=>x+y)

val rdd4=rdd3.sortBy(x=>x._2,false)
val premium_custome=rdd4.filter(x=>x._2>5000)
//rdd4.saveAsTextFile("C:/Users/prabh/Downloads/output123")
rdd4.collect().foreach(println)
scala.io.StdIn.readLine()
}