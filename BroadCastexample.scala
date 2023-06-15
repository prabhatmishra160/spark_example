
import org.apache.spark.SparkContext
import scala.io.Source


object BroadCastexample extends App {
  
  System.setProperty("hadoop.home.dir","C:/Hadoop")

  
  
  
def loadBoringWords():Set[String]={
 var boringwords:Set[String] = Set()
 val line=Source.fromFile("C:/Users/prabh/Downloads/sdata/boringwords.txt").getLines()
 for(lin<-line){
   
   boringwords += lin
 }
 
  boringwords
}
  
 val sc = new SparkContext("local[*]","wordcount")
var nameSet=sc.broadcast(loadBoringWords)


val rdd1 = sc.textFile("C:/Users/prabh/Downloads/sdata/campaigndata.csv")

val rdd2=rdd1.map(x=>(x.split(",")(10).toFloat,x.split(",")(0)))

val rdd3=rdd2.flatMapValues(x=>x.split(" "))

val rdd4=rdd3.map(x=>(x._2.toLowerCase(),x._1))

val rdd5=rdd4.filter(x => !nameSet.value(x._1))
val rdd6=rdd5.reduceByKey((x,y)=>x+y).sortBy(x=>x._2,false)
rdd6.collect.foreach(println)
  
}