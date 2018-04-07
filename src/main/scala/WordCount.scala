import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("Invalid number of arguments supplied")
      System.exit(1)
    }


    val master = args(0)
    val source = args(1)
    val destination = args(2)

    println(s"Supplied arguments $source, $destination, $master")
    val config = new SparkConf().setAppName("WordCount").setMaster(master)
    val sparkContext = new SparkContext(config)

    val textFile = sparkContext.textFile(source)
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _).sortByKey()
    counts.saveAsTextFile(destination)
  }
}
