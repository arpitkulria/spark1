import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object assignment1 extends App {
  val conf = new SparkConf().setAppName("appName").setMaster("local")
  val sc = new SparkContext(conf)
  val pagecounts = sc.textFile("/home/arpit/Documents/Spark/pagecounts")
  val enCountRDD = pagecounts.flatMap(_.split(' ')).filter(_.contains("/en/"))
  val enCount = enCountRDD.collect.length
  println(s"\n\n--page count RDD ---${pagecounts}--------")
  println(s"\n\n--en counts ---${enCount}--------")
  println(s"\n\n--en counts RDD---${enCountRDD}--------")
  println("\n\n Take 10 " + pagecounts.take(10).toList)
  println(s"\n\n --count---${pagecounts.count()}--------")

  

}