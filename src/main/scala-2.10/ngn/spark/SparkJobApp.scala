package ngn.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

trait SparkJobApp extends App with JobRunner {
  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("cross-validation")

  implicit val sc = new SparkContext(conf)
  implicit val sqlContext = new SQLContext(sc)

  sc.setLogLevel("FATAL")
}

