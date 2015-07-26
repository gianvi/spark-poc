package ngn.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

abstract trait SparkJob[U] {
  var next: Option[(U) => SparkJob[_]] = None

  def execute(implicit sc: SparkContext): U

  def run(implicit sc: SparkContext): Unit = {
    next match {
      case None => execute
      case Some(e) => e(execute).run(sc)
    }
  }

  def andThen[T <: SparkJob[_]](job: (U) => T)(implicit sc: SparkContext) = {
    next = Some(job)

    this
  }
}
