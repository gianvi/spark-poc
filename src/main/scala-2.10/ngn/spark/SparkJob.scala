package ngn.spark

import org.apache.spark.{SparkConf, SparkContext}

abstract trait SparkJob {
  def run(sc: SparkContext): Unit
}
