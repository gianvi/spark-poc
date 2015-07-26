package jobs

import ngn.spark.{JobRunner, SparkJob}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext

class HeadlessCsvToLabeledPoints extends SparkJob with Serializable with JobRunner {
  def bool2Double(bool: Boolean) =
    if (bool) 1.0 else 0.0

  def execute(implicit sc: SparkContext): Unit = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

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
      .filterNot(_ == "q20_1_5")

    // Create binary data
    val df = data
      .map(_.split(",").toList)
      .map { l =>
      ( if(l.contains("q20_1_5")) 1.0 else 0.0,
        headers
          .map { h =>
          l.contains(h)
        }
          .map(bool2Double)
        )
    }

      .map(v => LabeledPoint(v._1, Vectors.dense(v._2.toArray)))
      .toDF()

    run(new LogisticRegressionJob(df))
  }
}
