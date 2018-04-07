import org.apache.spark.{SparkConf, SparkContext}

object SparkNYSE {

  val STOCK_SYMBOL: Int = 1
  val HIGH_PRICE: Int = 4
  val LOW_PRICE: Int = 5

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: SparkNYSE <input> <output> <numOutputFiles>")
      System.exit(1)
    }


    val master = args(0)
    val source = args(1)
    val destination = args(2)
    val numberOfOutputFiles = args(3).toInt

    val sparkConf = new SparkConf().setAppName("Spark NYSE").setMaster(master)
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.textFile(source)
      .map(line => line.split(","))
      .filter(_.length > 5)
      .map(x => (x(STOCK_SYMBOL), (x(HIGH_PRICE).toFloat -
        x(LOW_PRICE).toFloat) * 100 / x(LOW_PRICE).toFloat))
      .reduceByKey(Math.max(_, _), numberOfOutputFiles)
      .sortByKey(ascending = true)
      .saveAsTextFile(destination)

    System.exit(0)
  }

}
