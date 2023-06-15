import org.apache.spark.SparkContext




object wordcount extends App {
  //local[*]=>local cluster * means use all core
  // word count=>application name
  System.setProperty("hadoop.home.dir","C:/Hadoop")
 
val sc = new SparkContext("local[*]","wordcount")

val rdd1 = sc.textFile("C:/Users/prabh/Downloads/sdata/wordcount.txt")

val rdd2= rdd1.flatMap(x=>x.split(" "))


val rdd3=rdd2.map(x=>(x.toLowerCase(),1))



val rdd4 = rdd3.reduceByKey((a,b)=>a+b)
val rdd5=rdd4.map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
//false to make order decending by default it is ascending
rdd5.collect().foreach(println)
val rdd6=rdd4.sortBy(x=>x._2)
rdd6.collect().foreach(println)
// top 10 most occuring word 
// sorting on values


scala.io.StdIn.readLine()
}