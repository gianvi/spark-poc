import ngn.spark._
import mbuto.ImagePipelines.KNN.readers.ImageLoader

/**
  * Created by gian on 05/05/16.
  */
object ImageApp extends SparkJobApp {

  val selectedPipeline = "imagePipeline"

  selectedPipeline match {

    case "imagePipeline" => run {
      new ImageLoader(
        path = "data/imagePipeline/imagecl_sample/train/"
      )
        .map {
          case _ => ()
        }
    }

    case "" =>  println("No pipelines selected!")

    case _ => println(s"This pipeline ${selectedPipeline.toUpperCase} is not yet implemented!")
  }


}