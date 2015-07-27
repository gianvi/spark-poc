package jobs

import ngn.spark.SparkJob
import ngn.spark.ml.SparkMl
import org.apache.spark.SparkContext
import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml._
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType

class LogisticRegression(df: DataFrame, featuresColumn: String, labelColumn: String)
  extends SparkJob[PipelineModel]
  with SparkMl
  with Serializable {

  def execute(implicit sc: SparkContext): PipelineModel = {
    // Split training and test data
    val training :: test :: Nil = splitData(
      df.withColumn("labelString", df(labelColumn).cast(StringType)), List(0.9, .1)
    ).map(_.cache)

    // Create linear regression and a pipeline
    val labelIndexer = new StringIndexer()
      .setInputCol("labelString")
      .setOutputCol("indexedLabel")

    val lir = new classification.LogisticRegression()
      .setFeaturesCol(featuresColumn)
      .setLabelCol("indexedLabel")
      .setRegParam(0.2)
      .setElasticNetParam(0.0)
      .setMaxIter(1000)
      .setTol(1E-6)

    val pipeline = new Pipeline().setStages(Array(
      labelIndexer, lir
    ))

    // Train
    val model = pipeline.fit(training)

    import ngn.spark.ml.Evaluators._
    evaluateRegressionModel(model, training, labelColumn)
    evaluateRegressionModel(model, test, labelColumn)

    model
  }

}
