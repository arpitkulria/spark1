
import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}


object CustomReceiver5 extends App {

  val sparkConf = new SparkConf().setAppName("CustomReceiver").setMaster("local[*]")
  val ssc = new StreamingContext(sparkConf, Seconds(10))

  val lines: ReceiverInputDStream[String] = ssc.receiverStream(new CustomReceiver5("/dir/path"))

  val words = lines.flatMap(_.split(" "))
  val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
  wordCounts.print()
  ssc.start()
  ssc.awaitTermination()
}


class CustomReceiver5(directory: String)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2)  {

  def onStart() {
    new Thread("Custom Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {}

  private def receive() {
    try {
      val files = getListOfFiles(directory)
      files map { file =>
        val reader = new BufferedReader(
          new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))

        var userInput = reader.readLine()
        while (!isStopped && userInput != null) {
          store(userInput)
          userInput = reader.readLine()
        }
      }
      restart("Trying to connect again")
    } catch {
      case t: Throwable =>
        restart("Error receiving data", t)
    }
  }

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
}