import jobs._
import ngn.spark._
import org.apache.spark.sql.DataFrame

object GwiApp extends SparkJobApp {

  val selectedPipeline = "kmeans"

  selectedPipeline match {

    case "kmeans" => run {
      new HeadlessCsvToLabeledPoints(
        path = "data/gwi.txt",
        labelColumn = "q20_1_5"
      )
        .flatMap {
          case df: DataFrame => new KMeansClustering(
            df = df,
            featuresColumn = "features"
          )
            .flatMap {
              case model => new EvaluateKMeansModel(
                model,
                df,
                featuresColumn = "features"
              )
                .map {
                  case _ => ()
                }
            }
        }
    }

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

    case "" =>  println("No pipelines selected!")

    case _ => println(s"This pipelines ${selectedPipeline}is not yet implemented")
  }
}



