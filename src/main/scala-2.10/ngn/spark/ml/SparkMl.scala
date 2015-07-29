package ngn.spark.ml

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

trait SparkMl {
  def load(path: String, dataFormat: String)(implicit sc: SparkContext, sqlContext: SQLContext) =
    new DataLoader(sc, sqlContext).load(path, dataFormat)

  def splitData(data: DataFrame, fractions: List[Double]): List[DataFrame] = {
    val totalFractions = fractions.reduceLeft(_ + _)
    require(totalFractions == 1, s"Fractions have to add up to 1 but were $totalFractions!")

    data.randomSplit(fractions.toArray, seed = 12345).toList
  }
}
