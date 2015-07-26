import jobs.{LogisticRegressionJob, HeadlessCsvToLabeledPoints}
import ngn.spark._
import org.apache.spark.sql.DataFrame

object GwiApp extends SparkJobApp {
  args.toList.head match {
    case "train" => run(
      new HeadlessCsvToLabeledPoints() andThen {
        new LogisticRegressionJob(_)
      }
    )
  }
}

