package ngn.spark.ml

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType

class ClassificationTransformer(fromColumn: String, toColumn: String) {
  def transform(data: DataFrame) =
    data.withColumn(toColumn, data(fromColumn).cast(StringType))
}
