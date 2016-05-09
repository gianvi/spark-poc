import mbuto.NeuralNetworks.EvaluatorNNDigitClassifier
import mbuto.NeuralNetworks.models.MLNN1BaseDigitClassifier
import mbuto.NeuralNetworks.readers.MnistPixelsReader
import ngn.spark.SparkJobApp

/**
  * Created by gian on 05/05/16.
  */
object NeuralApp extends SparkJobApp {

  val selectedPipeline = "BaseMultilayer"

  selectedPipeline match {

      case "BaseMultilayer" => run {
        new MnistPixelsReader(
          path = "data/MnistCsv/mnist_train.csv"
        ).flatMap {

          case training => new MnistPixelsReader(
            path = "data/MnistCsv/mnist_test.csv"
          ).flatMap {

            case test => new MLNN1BaseDigitClassifier(
              train = training
            ).flatMap {

              case pipeline1 => new EvaluatorNNDigitClassifier(
                pipeline1,
                train = training,
                test = test
              ).map {

                case _ => ()
              }
            }
          }
        }
      }

      case "" =>  println("No pipelines selected!")

      case _ => println(s"This pipeline ${selectedPipeline.toUpperCase} is not yet implemented!")
    }


}
