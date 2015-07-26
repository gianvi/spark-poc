package jobs

import ngn.spark.{JobRunner, SparkJob}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{DataFrame, SQLContext}

class HeadlessCsvToLabeledPoints extends SparkJob[DataFrame] with Serializable {
  def bool2Double(bool: Boolean) =
    if (bool) 1.0 else 0.0

  def execute(implicit sc: SparkContext): DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val label = "q20_1_5"

    // Load data
    val data = sc
      .textFile("data/gwi.txt")
      .coalesce(1)
      .cache()

    // Extract unique headers
    val headers = data
      .map(_.split(",").toList)
      .flatMap(l => l)
      .distinct()
      .collect()
      .toList
      .sorted
      .filterNot(_ == label)

    // Create binary data
    data
      .map(_.split(",").toList)
      .map { l =>
      val features = headers
        .map { h =>
        l.contains(h)
      }
        .map(bool2Double)
        .toArray

      LabeledPoint(if(l.contains(label)) 1.0 else 0.0, Vectors.dense(features))
    }
      .toDF()
  }
}
