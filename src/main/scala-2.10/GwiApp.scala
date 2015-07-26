import jobs.{HeadlessCsvToLabeledPoints, LogisticRegressionJob}
import ngn.spark._
import ngn.spark.ml.SparkMl
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.{SparkConf, SparkContext}

object GwiApp extends SparkJobApp {
  args.toList.head match {
    case "train" => run(new HeadlessCsvToLabeledPoints)
  }
}

