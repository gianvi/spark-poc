package ngn.spark

import org.apache.spark.{SparkConf, SparkContext}

trait SparkJobApp extends App {
  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("cross-validation")

  val sc = new SparkContext(conf)

  def run[T <: SparkJob](job: T) = {
    try {
      job.run(sc)
    } finally {
      sc.stop()
    }
  }
}
