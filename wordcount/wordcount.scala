import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._ 
import org.apache.spark.SparkConf

object SparkWordCount { 
    
    def main(args: Array[String]) {

        System.out.println("Starting...")

        val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
        val threshold = args(1).toInt

        val tokenized = sc.textFile(args(0)).flatMap(_.split(" "))
        val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)   
        val filtered = wordCounts.filter(_._2 >= threshold)
        val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _) 
    
        System.out.println(charCounts.collect().mkString(", "))
    } 
}