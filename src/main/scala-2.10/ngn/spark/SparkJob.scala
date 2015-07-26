package ngn.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

abstract trait SparkJob[U] {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  var next: Option[(U) => SparkJob[_]] = None

  def execute(implicit sc: SparkContext): U

  def run(implicit sc: SparkContext): Unit = {
    val start = System.currentTimeMillis()
    log.info(s"job execution started")
    val res = execute
    log.info(s"job execution done in ${(System.currentTimeMillis() - start) / 100 / 10.0}s")

    next match {
      case None => execute
      case Some(e) =>
        val job = e(res)
        log.info(s"Next job ${job.getClass} detected")

        job.run(sc)
    }
  }

  def andThen[T <: SparkJob[_]](job: (U) => T)(implicit sc: SparkContext) = {
    next = Some(job)

    this
  }
}
