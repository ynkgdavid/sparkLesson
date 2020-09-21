import org.apache.spark.{SparkConf, SparkContext}

object BroadcastVariableScala {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf()
      .setAppName("Broadcast")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val factor = 3;
    val factorBroadcast = sc.broadcast(factor)
    val numberArray = Array(1,2,3,4,5)
    val numbers = sc.parallelize(numberArray,1)
    val multipleNumber = numbers.map(num=>num*factorBroadcast.value)
    multipleNumber.foreach(num=>println(num))
  }
}
