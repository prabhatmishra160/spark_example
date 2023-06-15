import org.apache.spark.SparkContext


object logexample extends App {
  val sc = new SparkContext("local[*]","wordcount")
  val mylist=List("WARN:fugierjg ehfo 4 4 ",
      "WARN:jkshfkwe ;lkfwe;ioh 2u9u",
      "ERROR:ufyhwqlk,mlkvo[lewpk",
      "WARN:asodwpidkwlm lk opifkpewkf' l jfkpwo")
   val rdd1=sc.parallelize(mylist) //create rdd
   
   val pairrdd=rdd1.map(x=>{
     
     val columns =x.split(":")
     val loglevel=columns(0)
      (loglevel,1)     
   })
   val result=pairrdd.reduceByKey((x,y)=>x+y)
   result.collect().foreach(println)
}