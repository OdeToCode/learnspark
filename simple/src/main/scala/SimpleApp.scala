import org.apache.spark.sql.SparkSession

object SimpleApp {
    def main(args: Array[String]) {
        val inputFile = "/usr/local/spark/spark-2.4.3-bin-hadoop2.7/README.md"
        val spark = SparkSession.builder.appName("Simple App").getOrCreate()
        val inputData = spark.read.textFile(inputFile)
        val numAs = inputData.filter(line => line.contains("a")).count()
        val numBs = inputData.filter(line => line.contains("b")).count()
        println(s"Lines with a: $numAs, Lines with b: $numBs")
        spark.stop()
    }
}