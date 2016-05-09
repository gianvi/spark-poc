package mbuto.AudienceGenderPrediction.readers

import ngn.spark.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by gian on 23/04/16.
  */
class UserSynthReader(path: String, minPartitions: Int = 8)
  extends SparkJob[DataFrame]
    with Serializable {


  val schemaStringUserSynth = "ts sid session taxonomy uuid is_new_user_id eid provider url referrer redirect request ip_device os language browser ua timezone ris co device device_model device_UID_MD5 device_UID_ODIN device_UID_OPENUDID device_UID_SHA1 carrier tablet_model mobile_model geo lat lng age birth_date gender city province state country zip tracker serving custom segments validity validity_time_unit advertiser_id job tag_id".split(" ");
  val schemaUserSynth = StructType(schemaStringUserSynth.map(fieldName => StructField(fieldName, StringType, true)))

  override def execute(implicit sc: SparkContext, sqlContext: SQLContext): DataFrame = {


    val userSynthDF = sqlContext
      .read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(schemaUserSynth)
      .load(path)
      .na.fill("SHOUL BE NULL")
      .cache()

    log.info(s"UserSynth LOADED: ${userSynthDF.count()}")

    userSynthDF.printSchema()
    userSynthDF.show()
    userSynthDF
  }

}
