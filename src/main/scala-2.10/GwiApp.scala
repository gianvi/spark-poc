import jobs._
import ngn.spark._
import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel

object GwiApp extends SparkJobApp {
  args.toList.head match {
    case "kmeans" => run(for {

        df <- new HeadlessCsvToLabeledPoints(
          path = "data/gwi.txt",
          labelColumn = "q20_1_5"
        )

        model <- new KMeansClustering(
          df = df,
          featuresColumn = "features"
        )

        _ <- new EvaluateKMeansModel(
          model, df, featuresColumn = "features")

      } yield ())

    case "logistic" => run(for {

        df <- new HeadlessCsvToLabeledPoints(
          path = "data/gwi.txt",
          labelColumn = "q20_1_5"
        )

        model <- new LogisticRegression(
          df = df,
          featuresColumn = "features",
          labelColumn = "label",
          testSetFraction = .4
        )

        _ <- new EvaluateModel(model)

      } yield ())
  }
}



