package mbuto.ImagePipelines.KNN

import com.typesafe.config.ConfigFactory

/**
  * Created by gian on 08/05/16.
  */

object ConfImageClassificator {
//  private val config =  ConfigFactory.load()
//
//  private lazy val root = config.getConfig("my_app")
//
//  object ModelConfig {
//    private val httpConfig = config.getConfig("http")
//
//    lazy val interface = httpConfig.getString("interface")
//    lazy val port = httpConfig.getInt("port")
//  }

  val NUM_TRAINING_IMAGES_PER_CLASS = 15

  val NUM_TEST_IMAGES_PER_CLASS = 15

  //indica la percentuale sul num complessivo dei kps
  val NUM_PERCENTAGE_CLUSTER = 1

  val SUBPATH_TO_SAVE = s"data/imagePipeline"
}