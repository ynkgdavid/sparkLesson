import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object wordcountscala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local");
    val sc = new SparkContext(conf)

    val lines = sc.textFile("c://public//pod.txt");
    val words = lines.flatMap{ line => line.split(" ")}
    val pairs = words.map{ word => (word, 1)}
    val wordCounts = pairs.reduceByKey{ _ + _ }

    wordCounts.foreach(wordCount=>println(wordCount._1 + " appeared" + wordCount._2))
  }
}