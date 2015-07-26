package jobs

import ngn.spark.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel

class EvaluateModelJob(model: PipelineModel) extends SparkJob[Unit] with Serializable {
  def execute(implicit sc: SparkContext): Unit =
    println(model.uid)
}
