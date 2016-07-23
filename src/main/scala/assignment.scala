import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object assignment1 extends App {
  val conf = new SparkConf().setAppName("appName").setMaster("local")
  val sc = new SparkContext(conf)
  //https://dumps.wikimedia.org/other/pagecounts-raw/2015/2015-12/pagecounts-20151201-220000.gz
  val pagecounts = sc.textFile("/pagecounts/file/path or use above link ")
  val enCountRDD = pagecounts.map(_.split(' ')).filter(x => x(0) == "en")
  val enCount = enCountRDD.count
  val q5Ans  = pagecounts.map (x => (x.split(' ')(1), x.split(' ')(2))).reduceByKey((a,b) => a+b).filter(_._2.toDouble > 200000)
  println(s" n\n\n\n\n\n\n    ANS question 5.count  >>>>   ${q5Ans.count()} \n\n\n\n\n\n")
  println(s" n\n\n\n\n\n\n    ANS question 5.take(100) >>>>   ${q5Ans.take(100).toList} \n\n\n\n\n\n")
  println(s"\n\n--page count RDD ---${pagecounts}--------")
  println(s"\n\n--en counts ---${enCount}--------")
  println(s"\n\n--en counts RDD---${enCountRDD}--------")
  println("\n\n Take 10 " + pagecounts.take(10).toList)
  println(s"\n\n --count---${pagecounts.count()}--------")
}