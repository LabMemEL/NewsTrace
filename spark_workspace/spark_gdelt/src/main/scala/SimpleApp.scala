// package gdelt.spark
import org.apache.spark.sql.SparkSession
// import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws, lit, collect_list, max}

// import scala.io.Source
import java.util.Properties
import com.typesafe.config.ConfigFactory

// import com.amazonaws.services.s3.AmazonS3Client
// import com.amazonaws.services.s3.model.GetObjectRequest
// import org.apache.spark.{SparkContext, SparkConf}
// import java.sql.{DriverManager, Connection}

object SimpleApp {
  val jdbc_path = ConfigFactory.load().getConfig("main").getString("jdbc")
  val url = ConfigFactory.load().getConfig("main").getString("url")
  val driver = ConfigFactory.load().getConfig("main").getString("driver")
  val username = ConfigFactory.load().getConfig("main").getString("user")
  val password = ConfigFactory.load().getConfig("main").getString("passwd")

  
  def load_S3_DF(spark: SparkSession, isEvent: Boolean, batch_unit : String): DataFrame = {
    //load either events or mentions data from S3 with yearly/monthly/daily/15-min batches
    val (sourcetype, extention) =
      if (isEvent) ("events/", "*.export.csv")
      else ("mentions/","*.mentions.csv")

    val loadedDF = spark.read.format("csv")
    .option("sep", "\t")
    .option("mode", "DROPMALFORMED")
    .load("s3a://insightdatarichard/v2/" + sourcetype + batch_unit + "18230000" + extention) //fileName; "s3://" triggers jets3t exception
    loadedDF
  }

  def write_DF_DB(records_df: DataFrame, tablename: String) {
    // load DB configurations
    val connectionProperties = new Properties()
    connectionProperties.put("user", username)
    connectionProperties.put("password", password)
    connectionProperties.put("driver", driver)

    records_df.write
          .mode("append")
          .jdbc(jdbc_path, "gdeltDB."+ tablename, connectionProperties)
  }

  def aggregate_mentions(spark: SparkSession, month: String): DataFrame = {
    // load mentions data and aggregate to unique event record
    val ex_key_cols = Seq("EID", "Init_Date", "Curr_Date", "Sc_URL")

    val mentionsDF = load_S3_DF(spark, isEvent = false, month)
    val pre_agg_DF = mentionsDF
      .select("_c0","_c1","_c2","_c5")
      .toDF(ex_key_cols: _*)
      .withColumn("Days_Passed", ((col("Curr_date").cast("long") - col("Init_Date").cast("long"))/86400)
      .cast("Int"))
      .select("EID", "Days_Passed", "Sc_URL")

    val aggregatedDF = pre_agg_DF
      .groupBy("EID")
      .agg(collect_list("Sc_URL") as "URLs", max(col("Days_Passed")) as "Coverage")
      .withColumn("URLs", concat_ws("\t", col("URLs")))
    aggregatedDF
  }

  def join_events(spark: SparkSession, exentionsDF: DataFrame, month: String): DataFrame = {
    // load unique events data and left join with mentions data
    val main_cols = Seq("EID", "Date", "act1", "act2", "N_Mentions", "N_Sources", "N_Discs", "URL")
    
    val eventsDF = load_S3_DF(spark, isEvent = true, month)
    val kwordsDF = eventsDF
      .select("_c0","_c1","_c6","_c16","_c31","_c32","_c33","_c60")
      .toDF(main_cols: _*)
      .withColumn("K_words", concat_ws(",", col("act1"), col("act2")))
      .drop("act1", "act2")
    val final_DF = kwordsDF
      .join(exentionsDF, Seq("EID"), "left_outer")
    final_DF
  }

  def batch_monthly(spark: SparkSession) {
    // val year = Array("2015, 2016, 2017")
    // val months = Array("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
    val year = "2015"
    val months = Array("02")
      for (month <- months) {
        val mentionsDF = aggregate_mentions(spark, year + month)
        val expandedDF = join_events(spark, mentionsDF, year + month)
        write_DF_DB(expandedDF, "FN_" + month)
      }
  }

  def main(args: Array[String]) {

    val spark = SparkSession
        .builder()
        .appName("GDELT_process")
        .getOrCreate()
    
    try {
      batch_monthly(spark)
    } catch {
      case e: Exception => e.printStackTrace
    }

    spark.stop()
  }
}