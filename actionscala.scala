import org.apache.spark.{SparkConf, SparkContext}

object actionscala {
  def main(args: Array[String]): Unit ={
//    reduce()
//    collect()
//    count()
//    take()
    countByKey()
  }

  def reduce(): Unit ={
    val conf = new SparkConf()
      .setAppName("reduce")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val numberArray = Array(1,2,3,4,5,6,7,8,9,10)
    val numbers = sc.parallelize(numberArray,1)
    val sum = numbers.reduce(_+_)
    println(sum)
  }
  def collect(): Unit ={
    val conf = new SparkConf()
      .setAppName("reduce")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val numberArray = Array(1,2,3,4,5,6,7,8,9,10)
    val numbers = sc.parallelize(numberArray,1)
    val doubleNumbers = numbers.map{num=>num*2}
    val doubleNumberArray = doubleNumbers.collect()
    for(num  <- doubleNumberArray){
      println(num)
    }
  }
  def count(): Unit ={
    val conf = new SparkConf()
      .setAppName("count")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val numberArray = Array(1,2,3,4,5,6,7,8,9,10)
    val numbers = sc.parallelize(numberArray,1)
    val sum = numbers.reduce(_+_)
    println(sum)
  }
  def take(): Unit ={
    val conf = new SparkConf()
      .setAppName("take")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val numberArray = Array(1,2,3,4,5,6,7,8,9,10)
    val numbers = sc.parallelize(numberArray,1)
    val top3Numbers = numbers.take(3)
    for(num<-top3Numbers){
      println(num)
    }

  }
  def countByKey(): Unit ={
    val conf = new SparkConf()
      .setAppName("countByKey")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val studentList = Array(
      Tuple2("class1","leo"),
      Tuple2("class1","jack"),
      Tuple2("class2","lion"),
      Tuple2("class2","leo2"),
      Tuple2("class2","leo3")
    )
    val students = sc.parallelize(studentList,1)
    val studentCounts = students.countByKey()
    println(studentCounts)
  }




}
