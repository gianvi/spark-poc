package jobs

import ngn.spark.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SQLContext

class EvaluateModel(model: PipelineModel) extends SparkJob[Unit] with Serializable {
  override def execute(implicit sc: SparkContext, sqlContext: SQLContext): Unit =
    println(model.uid)
}
