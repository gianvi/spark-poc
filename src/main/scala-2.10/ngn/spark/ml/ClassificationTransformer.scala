package ngn.spark.ml

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType

/**
 * Created by petr on 25/07/2015.
 */
class ClassificationTransformer(fromColumn: String, toColumn: String) {
  def transform(data: DataFrame) =
    data.withColumn(toColumn, data(fromColumn).cast(StringType))
}
