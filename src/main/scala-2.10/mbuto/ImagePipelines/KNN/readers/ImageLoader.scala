package mbuto.ImagePipelines.KNN.readers

import java.io.{File, IOException}
import org.apache.spark.rdd.RDD

import collection.JavaConverters._

import mbuto.ImagePipelines.KNN.ConfImageClassificator
import mbuto.ImagePipelines.myOpenImaj.ImageDataModels.{LocalTypeData, LocalImageData}
import ngn.spark.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext}

/**
  * Created by gian on 08/05/16.
  */
class ImageLoader(path: String, minPartitions: Int = 8)
  extends SparkJob[Unit]
    with Serializable {

  override def execute(implicit sc: SparkContext, sqlContext: SQLContext): Unit = {

    val files: List[LocalTypeData] = loader(path)

    val filesRDD: RDD[LocalTypeData] = sc.parallelize(files)

    //files.take(5).foreach(println)
  }


  /**
    * First stage of the image recognition process. Writes each image path and its
    * corresponding classification type to a file.
    * <p>
    * Input:
    * - The root directory of the training data
    * Ouput:
    * - file containing path/type pairs for each image:
    * img1Path car
    * img2Path car
    * img3Path bicycle
    * img4Path bicycle
    * img5Path motorbike
    * img6Path motorbike
    * ....
    */
  @throws(classOf[Exception])
  def loader(dir: String): List[LocalTypeData] = {

    val d = new File(dir)
    if (d.exists && d.isDirectory) {

      val classes = d.listFiles.filter(_.isDirectory).toList
      println(s"Nel dataset ${d.getPath} di training ci sono  ${classes.size} gruppi d'immagini:")
      classes.foreach(x => println(" - "+x.getPath))
      println

      val trainingData: List[LocalTypeData] = classes.flatMap {
        x =>
          require(x.isDirectory)

          x
            .listFiles()
            .toList
            .slice(0, ConfImageClassificator.NUM_TRAINING_IMAGES_PER_CLASS)
            .map(f => new LocalTypeData(f.getAbsolutePath, x.getName))
      }

      LocalImageData.writeList(s"${ConfImageClassificator.SUBPATH_TO_SAVE}/myImageData/1traindata.txt", trainingData.asJava)
      trainingData
    }
    else {
      List[LocalTypeData]()
    }
  }
}