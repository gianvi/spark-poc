package ngn.spark

import org.apache.spark.{SparkConf, SparkContext}

abstract trait SparkJob {
  def execute(implicit sc: SparkContext): Unit
}
