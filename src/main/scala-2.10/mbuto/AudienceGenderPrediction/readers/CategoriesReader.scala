package mbuto.AudienceGenderPrediction.readers

import ngn.spark.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by gian on 24/04/16.
  */
class CategoriesReader(path: String, minPartitions: Int = 8)
  extends SparkJob[DataFrame]
    with Serializable {

  val urlMD5 = "urlmd5"
  val categoryId = "category_id"
  val scoreCat = "score_cat"
  val rank = "rank"


  override def execute(implicit sc: SparkContext, sqlContext: SQLContext): DataFrame = {

    val sitesDF = sqlContext
      .read
      .parquet(path)
      .withColumn(urlMD5, (col(urlMD5)).cast(StringType))
      .withColumn(scoreCat, (col(scoreCat).cast(DoubleType)))


    //assert(sitesDF.filter(col(scoreCat)>(1)).count()==0, message = "C'è qualche score cate > 1!")
    //assert(sitesDF.groupBy(col(urlMD5)).agg(countDistinct(categoryId)).filter("COUNT(DISTINCT category_id) > 3").count()==0, message = "C'è un sito che appartiene a piu di 3 categorie!")

    //TODO risolvere il problema dei doppioni (url, catId)!
    //sitesDF.groupBy(col(urlMD5)).agg(count(col(categoryId)).as("count_catId"), countDistinct(categoryId).as("distCount_catId")).orderBy(desc("distCount_catId"), desc("count_catId")).show(30)
    //sitesDF.groupBy(col(urlMD5)).agg(count(categoryId), countDistinct(categoryId), count(scoreCat), countDistinct(scoreCat), sum(scoreCat)).filter("sum(score_cat) > 1").show(20)

    //println(s"sites LOADED: ${sitesDF.count()}")
    //sitesDF.printSchema()
    //sitesDF.show()
    val sample = sitesDF
        .sample(withReplacement = false, 0.001, System.currentTimeMillis().toInt)

    println(s"Sample from sites reader with ${sample.select(categoryId).distinct().count()}/23 distinct categories")
    sample

  }

}
