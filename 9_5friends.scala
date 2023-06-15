import org.apache.spark.SparkContext


object friends extends App {
  // row_id,name , age, number_of_connection
  //average number of connection for each age
  
  
  
  def parseLine(line:String)={
   val fields=line.split("::")
   val age =fields(2).toInt
   val numFriends=fields(3).toInt
   (age,numFriends)
    
  }
  
System.setProperty("hadoop.home.dir","C:/Hadoop")
 
val sc = new SparkContext("local[*]","wordcount")

val rdd1 = sc.textFile("C:/Users/prabh/Downloads/sdata/friendsdata.csv")

val rdd2=rdd1.map(parseLine)

val rdd3=rdd2.mapValues(x=>(x,1))

//val rdd3=rdd2.map(x=>(x._1,(x._2,1)))

val rdd4=rdd3.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))

//val rdd5=rdd4.map(x=>(x._1,x._2._1/x._2._2)).sortBy(_._2)
val rdd5 = rdd4.mapValues(x=>x._1/x._2).sortBy(x=>x._2)
//mapValue instead of taking whole row only take values from each row 

rdd5.collect().foreach(println) 
}