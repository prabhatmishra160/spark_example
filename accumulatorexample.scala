import org.apache.spark.SparkContext


object accumulatorexample extends App {
  // read number of blanks line
  
  System.setProperty("hadoop.home.dir","C:/Hadoop")
  val sc = new SparkContext("local[*]","wordcount")
  
  val rdd1 = sc.textFile("C:/Users/prabh/Downloads/sdata/sample.txt")
  val myaccum =sc.longAccumulator("blank line accumulator")
  rdd1.foreach(x=> if (x=="") myaccum.add(1))
  println("***************--------------****************" )
  println(myaccum.value)
   println("")
  println("***************--------------****************" )
  
}