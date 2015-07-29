package jobs

import ngn.spark.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.{Vector => Vec}
import org.apache.spark.sql.DataFrame

class EvaluateKMeansModel(model: KMeansModel, df: DataFrame, featuresColumn: String) extends SparkJob[Unit] with Serializable {
  def execute(implicit sc: SparkContext): Unit =
    val rdd = df
      .select(df(featuresColumn))
      .map(_(0).asInstanceOf[Vec])

    val WSSSE = model.computeCost(rdd)

    println("Within Set Sum of Squared Errors = " + WSSSE)
}
