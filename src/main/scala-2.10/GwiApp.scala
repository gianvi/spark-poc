import jobs.{EvaluateModel, HeadlessCsvToLabeledPoints, LogisticRegression}
import ngn.spark._

object GwiApp extends SparkJobApp {
  run(for {
    df <- new HeadlessCsvToLabeledPoints(
      path = "data/gwi.txt",
      labelColumn = "q20_1_5"
    )

    model <- new LogisticRegression(
      df = df,
      featuresColumn = "features",
      labelColumn = "label"
    )

    _  <- new EvaluateModel(model)
  } yield ())
}
