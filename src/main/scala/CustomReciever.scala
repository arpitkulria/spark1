import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver


object CustomReceiver extends App {

  val sparkConf = new SparkConf().setAppName("CustomReceiver").setMaster("local")
  val ssc = new StreamingContext(sparkConf, Seconds(2))

  val lines = ssc.receiverStream(new CustomReceiver(ssc))
  //ssc.start()

  //  val words = lines.flatMap(_.split(" "))
//  val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)


  lines.print()

  val sns = lines.foreachRDD(x => println(x.count()))

  println(s">>>>/\n\n\n\n sns >>> ${sns}")
  lines.start()
  ssc.start()

  //lines.saveAsTextFiles("/home/arpit/Documents/test/res")
  //ssc.checkpoint("/home/papillon/prova/checkpoint")
//  ssc.checkpoint("/home/arpit/Documents/test/check")
  //ssc.start()
  ssc.awaitTermination()
}



class CustomReceiver(ssc: StreamingContext) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  println("Demo ------------")
  def onStart() {
    println("--------------")
    // Start the thread that receives data over a connection
    new Thread("Custom Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    println("-----in stop function---------")
    //sc.stop
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    try {
    ssc.textFileStream("/home/arpit/Documents/test")

   /* //arr.print()

    arr.saveAsTextFiles("/home/arpit/Documents/test/res/")
    //ssc.checkpoint("/home/papillon/prova/checkpoint")
    ssc.checkpoint("/home/arpit/Documents/test/check/")
    ssc.start()*/
    //restart("Trying to connect again")
      //sc.stop()
    } catch {
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
      case _ => restart("Trying to connect again")
    }

    }
  }

