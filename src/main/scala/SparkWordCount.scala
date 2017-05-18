package main.scala
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
object SparkWordCount {
  def main(args: Array[String]) {
//
//    // get threshold
//    val threshold = args(1).toInt
//
//    // read in text file and split each document into words
//    val tokenized = sc.textFile(args(0)).flatMap(_.split(" "))
//
//    // count the occurrence of each word
//    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)
//
//    // filter out words with fewer than threshold occurrences
//    val filtered = wordCounts.filter(_._2 >= threshold)
//
//    // count characters
//    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)
//
//    System.out.println(charCounts.collect().mkString(", "))

    val max = args(0).toInt
    val partition =args(1).toInt
    val appName = args(2)
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName(appName))
    System.out.println("max number " +max)
    val composite1=sc.parallelize(1 to max,partition).map(p=>(-p,p)).groupByKey()
    composite1.foreach(println)
    System.out.println("stop")

  }
}
