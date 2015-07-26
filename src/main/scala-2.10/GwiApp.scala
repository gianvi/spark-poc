import ngn.spark._
import ngn.spark.ml.SparkMl
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.{SparkConf, SparkContext}

object GwiApp extends SparkJobApp {
  args.toList.head match {
    case "train" => run(new TrainJob)
  }
}

class TrainJob extends SparkJob with SparkMl with Serializable {
  def run(sc: SparkContext) = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Load data
    val data = sc
      .textFile("data/gwi.txt")
      .coalesce(1)
      .cache()

    // Extract unique headers
    val headers = data
      .map(_.split(",").toList)
      .flatMap(l => l)
      .distinct()
      .collect()
      .toList
      .sorted

    // Create binary data
    val df = data
      .map(_.split(",").toList)
      .map { l =>
      headers.map { h => l.contains(h) }
    }

      .map(_.map(bool2Double))
      .map(v => LabeledPoint(v.head, Vectors.dense(v.toArray)))
      .toDF()



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

  def bool2Double(bool: Boolean) =
    if (bool) 1.0 else 0.0
}