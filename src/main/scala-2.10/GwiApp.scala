import ngn.spark._
import ngn.spark.ml.SparkMl
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

object GwiApp extends SparkJobApp {
  args.toList.head match {
    case "headers" => run(new ExtractHeadersJob)
    case "train" => run(new TrainJob)
  }
}

class TrainJob extends SparkJob with Serializable {
  def run(sc: SparkContext) = {
    val headers = "q10_1, q10_2, q10_3, q10_4, q10_5, q2_1, q2_2, q3_16, q3_17, q3_18, q3_19, q3_20, q3_21, q3_22, q3_23, q3_24, q3_25, q3_26, q3_27, q3_28, q3_29, q3_30, q3_31, q3_32, q3_33, q3_34, q3_35, q3_36, q3_37, q3_38, q3_39, q3_40, q3_41, q3_42, q3_43, q3_44, q3_45, q3_46, q3_47, q3_48, q3_49, q3_50, q3_51, q3_52, q3_53, q3_54, q3_55, q3_56, q3_57, q3_58, q3_59, q3_60, q3_61, q3_62, q3_63, q3_64, q6_1, q6_2, q6_3, q6_4, q6_5, s2_1, s2_27, s2_33, s2_353, s2_44, s2_46, s2_49, s2_61, s2_62, s2_65, s2_81, s2_82, s2_91".split(", ").toList

    def calculate(headers: List[String], values: List[String]) =
      headers.map(_ => values.contains("a"))

    def bool2Dobule(bool: Boolean) =
      if (bool) 1.0 else 0.0

    sc
      .textFile("data/gwi.txt")
      .map(_.split(",").toList)
      .map { l =>
      headers.map(h => l.contains(h))
    }
      .map(_.map(bool2Dobule))
      .saveAsTextFile("data/processed.txt")
  }
}

class ExtractHeadersJob extends SparkJob with SparkMl {
  def run(sc: SparkContext) = {
    //  import sqlContext.implicits._

    val schema = StructType(
      List("key").map(fieldName => StructField(fieldName, StringType, true))
    )

    val headers = sc
      .textFile("data/gwi.txt")
      .coalesce(1)
      .map(_.split(",").toList)
      .flatMap(l => l)
      .distinct()
      .collect()
      .toList
      .sorted

    println(headers)
  }
}