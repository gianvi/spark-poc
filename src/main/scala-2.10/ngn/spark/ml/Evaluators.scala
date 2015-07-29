package ngn.spark.ml

import org.apache.spark.ml.Transformer
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.DataFrame

object Evaluators {
  def evaluateRegressionModel(model: Transformer, data: DataFrame, labelColName: String): Unit = {
    val fullPredictions = model.transform(data).cache()
    val predictions = fullPredictions.select("prediction").map(_.getDouble(0))
    val labels = fullPredictions.select(labelColName).map(_.getDouble(0))
    val RMSE = new RegressionMetrics(predictions.zip(labels)).rootMeanSquaredError

    println(s"  Root mean squared error (RMSE): $RMSE")
  }
}
