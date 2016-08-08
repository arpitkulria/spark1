import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}


object assignment2 extends App {
  val conf = new SparkConf().setAppName("appName").setMaster("local")
  val sc = new SparkContext(conf)

  val spark = SparkSession.builder().appName("appName").getOrCreate()

  import spark.implicits._

  val csv: DataFrame = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/home/arpit/Documents/Spark/Fire_Department_Calls_for_Service.csv")

  //1st
  val first = csv.map(x => x.getString(3)).distinct.count
  println(s"First  = ${first}")

  //2nd
  val second = csv.select(csv("Call Type")).rdd.map(x => (x,1)).reduceByKey(_ + _).collect()
  println(s"Second  = ${second}")

  //3rd
  val minYear = csv.map(_.getString(4)).map(_.split("/")).map(x => x(2).toInt).rdd.min
  val maxYear = csv.map(_.getString(4)).map(_.split("/")).map(x => x(2).toInt).rdd.max
  val third  = maxYear - minYear
  println(s"Third  = ${third}")

  //4th
  csv.createOrReplaceTempView("fire_dept")
  val fourth = spark.sql("SELECT count(*) from fire_dept where 'Call Date' between (select DATE_SUB(MAX('Call Date'), 7) from fire_dept) AND (SELECT MAX('Call Date') from fire_dept)").show
  println(s"Fourth  = ${fourth}")

  //5th
  val fifth = csv.map(_.getString(4)).map(_.split("/")).map(x => x(2).toInt).filter(x => x == 2015).count
  println(s"Fifth  = ${fifth}")


}