package ngn.spark

import org.apache.spark.SparkContext

trait JobRunner {
  def run[T <: SparkJob[_]](job: T)(implicit sc: SparkContext): Unit = {
    try {
      job.run(sc)
    } finally {
      sc.stop()
    }
  }
}
