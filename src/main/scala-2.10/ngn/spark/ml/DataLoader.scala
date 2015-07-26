package ngn.spark.ml

import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by petr on 25/07/2015.
 */
class DataLoader(sc: SparkContext, sqlContext: SQLContext) {
  def load(path: String, format: String): DataFrame = {
    import sqlContext.implicits._

    val rdd = format match {
      case "dense" => MLUtils.loadLabeledPoints(sc, path)
      case "libsvm" => MLUtils.loadLibSVMFile(sc, path)
      case _ => throw new IllegalArgumentException(s"Bad data format: $format")
    }

    rdd.toDF()
  }
}
