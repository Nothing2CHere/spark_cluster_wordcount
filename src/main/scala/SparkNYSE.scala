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

    val sparkConf = new SparkConf().setAppName("Spark NYSE")
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.textFile(args(0))
      .map(line => line.split(","))
      .filter(_.length > 5)
      .map(x => (x(STOCK_SYMBOL), (x(HIGH_PRICE).toFloat -
        x(LOW_PRICE).toFloat) * 100 / x(LOW_PRICE).toFloat))
      .reduceByKey(Math.max(_, _), 1)
      .sortByKey(ascending = true)
      .saveAsTextFile(args(1))

    System.exit(0)
  }

}
