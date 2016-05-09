package mbuto.AudienceGenderPrediction.readers

import ngn.spark.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by gian on 23/04/16.
  */
class AdvReader(path: String, minPartitions: Int = 8)
  extends SparkJob[DataFrame]
    with Serializable {


  val schemaStringAdv = "ts sid session taxonomy uuid is_new_user_id eid provider url referrer redirect request ip_device os language browser ua timezone ris co device device_model device_UID_MD5 device_UID_ODIN device_UID_OPENUDID device_UID_SHA1 carrier tablet_model mobile_model geo lat lng age birth_date gender city province state country zip tracker serving custom segments validity validity_time_unit action action_type action_value campaign_id creative_id advertiser_id creative_size placement_id order_id order_code line_item_id inventory_id site_id link_advertising publisher_id external_app_id supply_type render_mode advertiserIDAndroid advertiserIDApple advertiserIDWindows price bid ext_segs_id job tag_id".split(" ");
  val schemaAdv: StructType = StructType(schemaStringAdv.map(fieldName => StructField(fieldName, StringType, true)))

  override def execute(implicit sc: SparkContext, sqlContext: SQLContext): DataFrame = {


    val advDF = sqlContext
      .read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", "\t")
        .option("nullValue", "SHOLD BE NULL")
      .schema(schemaAdv)
      .load(path)
      //.na.fill("SHOUL BE NULL")
      .cache()


    log.fatal(s"ADV LOADED: ${advDF.count()}")


    advDF.printSchema()
    advDF.show()
    advDF
  }

}

