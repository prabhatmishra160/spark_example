import org.apache.spark._
import org.apache.log4j.Level
import org.apache.log4j.Logger

object wordcountamazon {
  def main(args:Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
val sc = new SparkContext()

val rdd1 = sc.textFile("s3n://customerazureretail/book-data.txt")

val rdd2= rdd1.flatMap(x=>x.split(" "))

//s3n://customerazureretail/book-data.txt
val rdd3=rdd2.map(x=>(x.toLowerCase(),1))



val rdd4 = rdd3.reduceByKey((a,b)=>a+b)
val rdd5=rdd4.map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
rdd5.collect().foreach(println)
val rdd6=rdd4.sortBy(x=>x._2)
rdd6.collect().foreach(println)
// top 10 most occuring word 
// sorting on values


scala.io.StdIn.readLine()
}
    
  }
