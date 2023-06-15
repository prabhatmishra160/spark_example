import org.apache.spark.SparkContext


object movie_ratings extends App {
  
  // At least 1000 people should have rated for that movie
  //average ratings > 4.5
System.setProperty("hadoop.home.dir","C:/Hadoop")

val sc = new SparkContext("local[*]","wordcount")

val rdd1 = sc.textFile("C:/Users/prabh/Downloads/sdata/week11/ratings.dat")
// user_id,movie_id,ratings,timestamp
val map_rdd=rdd1.map(x=>{
  val fields=x.split("::")
  (fields(1),fields(2))
})
  val rdd3=map_rdd.mapValues(x=>(x.toFloat,1.0))
  val rdd4=rdd3.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
  val rdd5=rdd4.filter(x=>x._2._2>10)
  val rdd6=rdd5.mapValues(x=>x._1/x._2).filter(x=>x._2>4)
  
  
  val movie_rdd = sc.textFile("C:/Users/prabh/Downloads/sdata/week11/movies.dat")
  val fild=movie_rdd.map(x=>{
  val fields=x.split("::")
  (fields(0),fields(1))
  
})
val rdd7= fild.join(rdd6)
// in pair rdd join based on key
//we want to print only movies name

val rdd8=rdd7.map(x=>x._2._1)

rdd8.collect().foreach(println)
scala.io.StdIn.readLine()

}