package jobs

import ngn.spark.SparkJob
import ngn.spark.ml.SparkMl
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType

class LogisticRegressionJob(df: DataFrame) extends SparkJob with SparkMl with Serializable {
  def execute(implicit sc: SparkContext): Unit = {
    // Split training and test data
    val training :: test :: Nil = splitData(
      df.withColumn("labelString", df("label").cast(StringType)), List(.85, .15)
    )

    // Create linear regression and a pipeline
    val labelIndexer = new StringIndexer()
      .setInputCol("labelString")
      .setOutputCol("indexedLabel")

    val lir = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("indexedLabel")
      .setRegParam(0.0)
      .setElasticNetParam(0.0)
      .setMaxIter(500)
      .setTol(1E-6)

    val pipeline = new Pipeline().setStages(Array(
      labelIndexer, lir
    ))

    // Train
    val model = pipeline.fit(training)

    import ngn.spark.ml.Evaluators._
    evaluateRegressionModel(model, training, "label")
    evaluateRegressionModel(model, test, "label")
  }
}
