import examples.mlPipelinePersistence
import ngn.spark.SparkJobApp

  object PersistPipelineApp extends SparkJobApp {

    val selectedPipeline = "persistPipeline"

    selectedPipeline match {

      case "persistPipeline" => run(for {

        _ <- new mlPipelinePersistence()

        } yield ())

      case "" =>  println("No pipelines selected!")

      case _ => println(s"This pipeline ${selectedPipeline.toUpperCase} is not yet implemented!")
    }
  }
