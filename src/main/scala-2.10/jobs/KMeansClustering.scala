package jobs

import ngn.spark.SparkJob
import ngn.spark.ml.SparkMl
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector => Vec}
import org.apache.spark.sql.DataFrame

class KMeansClustering(df: DataFrame, featuresColumn: String)
  extends SparkJob[KMeansModel]
  with SparkMl
  with Serializable {

  def execute(implicit sc: SparkContext): KMeansModel = {
    val rdd = df
      .select(df(featuresColumn))
      .map(_(0).asInstanceOf[Vec])

    // Create linear regression and a pipeline
    val kmeans = new KMeans()
      .setMaxIterations(50)
      .setK(10)

    kmeans.run(rdd)
  }

}
