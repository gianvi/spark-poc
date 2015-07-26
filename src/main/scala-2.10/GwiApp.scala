import jobs.{EvaluateModelJob, HeadlessCsvToLabeledPoints, LogisticRegressionJob}
import ngn.spark._

object GwiApp extends SparkJobApp {
  args.toList.head match {
    case "train" => run(
      new HeadlessCsvToLabeledPoints() andThen {
        new LogisticRegressionJob(_, "features", "label") andThen {
          new EvaluateModelJob(_)
        }
      }
    )
  }
}