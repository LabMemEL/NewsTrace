// package gdelt.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws, lit}
import scala.io.Source
import java.util.Properties
// import com.amazonaws.services.s3.AmazonS3Client
// import com.amazonaws.services.s3.model.GetObjectRequest
import org.apache.spark.{SparkContext, SparkConf}
import java.sql.{DriverManager, Connection}

object SimpleApp {

  def create_tb_events(connection: Connection, date: String){
    // val driver =
    // val jdbcDF = SQLContext.load("jdbc", Map(
    //   "url" -> "jdbc:postgresql://ec2-54-202-161-91.us-west-2.compute.amazonaws.com/post"
    // ))
    // var connection:Connection = _
      val statement = connection.createStatement
      val update = "CREATE TABLE IF NOT EXISTS TB_" + date + """ (
                    EID INT PRIMARY KEY,
                    Date DATETIME NOT NULL,
                    K_words TEXT,
                    N_Mentions INT,
                    N_Sources INT,
                    N_Discs INT,
                    URL TEXT NOT NULL
                    )  ENGINE=INNODB;"""
      val rs = statement.executeUpdate(update)
  }


  def create_tb_mentions(connection: Connection, date: String){
    val statement = connection.createStatement
    val update = "CREATE TABLE IF NOT EXISTS MT_" + date + """ (
                  EID INT,
                  Init_Date DATETIME,
                  Curr_Date DATETIME,
                  Sc_Name TEXT NOT NULL,
                  Sc_URL TEXT,
                  Conf INT
                  )  ENGINE=INNODB;"""
    val rs = statement.executeUpdate(update)
  }

  def create_tb_finals(connection: Connection, date: String){
    // val limit_st = "SET GROUP_concat_max_len=15000"

    val statement = connection.createStatement
    val concate_st = "CREATE TABLE IF NOT EXISTS EX_" + date + """
                      SELECT mt.EID AS EID, COUNT('tb.EID') AS S_nums,
                      MAX(timestampdiff(minute, mt.Init_Date, mt.Curr_date)) AS max_span,
                      GROUP_CONCAT(mt.Sc_Name SEPARATOR '\t') AS S_names,
                      GROUP_CONCAT(mt.Sc_URL SEPARATOR '\t') AS S_urls
                      FROM MT_""" + date + " mt, TB_" + date + """ tb
                      WHERE tb.EID = mt.EID
                      GROUP BY tb.EID"""
    statement.executeUpdate(concate_st)

    val join_st = "CREATE TABLE IF NOT EXISTS RS_" + date + """
                  SELECT * FROM TB_""" + date + " LEFT JOIN EX_" + date + " USING(EID)"
    statement.executeUpdate(join_st)
  }

  def db_write(records_df: DataFrame, tablename: String) {  //update(records_df: DataFrame)

    val connectionProperties = new Properties()
    connectionProperties.put("user", "sparkMS")
    connectionProperties.put("password", "test")
    connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver")

    // val jdbcDF2 = sparkSes_db.read
    //   .jdbc("jdbc:mysql://ec2-54-202-161-91.us-west-2.compute.amazonaws.com:3306/gdeltDB", "gdeltDB.tasks", connectionProperties)
    // jdbcDF2.write
    //   .mode("append")
    //   .jdbc("jdbc:mysql://ec2-54-202-161-91.us-west-2.compute.amazonaws.com:3306/gdeltDB", "gdeltDB.tasks2", connectionProperties)
    records_df.write
          .mode("append")
          .jdbc("jdbc:mysql://ec2-54-202-161-91.us-west-2.compute.amazonaws.com:3306/gdeltDB", "gdeltDB."+ tablename, connectionProperties)
  }

  def process_mentions(connection: Connection, spark: SparkSession){
    val names = Seq("EID", "Init_Date", "Curr_Date", "Sc_Name", "Sc_URL", "Conf")
    val fileNames = Source.fromFile("mentionslist.out").getLines.toList.take(2)
    for (fileName <- fileNames) {
      // v2/events/20150715014500.export.csv into 2015_07_15
      val date = fileName.split("/")(2).substring(0,8)
      val month = date.substring(0,6)
      create_tb_mentions(connection, date) //consider if check to run only monthly

      val full_df = spark.read.format("csv")
          .option("sep", "\t")
          // .option("header", "true") //first line in file has headers
          .option("mode", "DROPMALFORMED")
          .load("s3a://insightdatarichard/"+ fileName) //s3 triggers jets3t exception
      // gdelt_full_df.show()

      val main_DF  = full_df.select("_c0","_c1","_c2","_c4","_c5","_c11")
      val renamed_df = main_DF.toDF(names: _*)
      // print(renamed_df.show(2))
      db_write(renamed_df, "MT_" + date)
      create_tb_finals(connection, date)
    }
  }

  def process_events(connection: Connection, spark: SparkSession){
    val names = Seq("EID", "Date", "act1", "act2", "N_Mentions", "N_Sources", "N_Discs", "URL")
    val fileNames = Source.fromFile("eventlist.out").getLines.toList.take(2)
    for (fileName <- fileNames) {
      // v2/events/20150715014500.export.csv into 2015_07_15
      val date = fileName.split("/")(2).substring(0,8)
      create_tb_events(connection, date)

      val full_df = spark.read.format("csv")
          .option("sep", "\t")
          // .option("header", "true") //first line in file has headers
          .option("mode", "DROPMALFORMED")
          .load("s3a://insightdatarichard/"+ fileName) //s3 triggers jets3t exception
      // gdelt_full_df.show()

      val main_DF  = full_df.select("_c0","_c1","_c6","_c16","_c31","_c32","_c33","_c60")
      val temp_df = main_DF.toDF(names: _*)
      val final_df = temp_df.withColumn("K_words", concat_ws(",", col("act1"), col("act2")))
                            .drop("act1", "act2")
      // print(final_df.show(2))
      db_write(final_df, "TB_" + date)
    }
  }

  def main(args: Array[String]) {

    val spark = SparkSession
        .builder()
        .appName("GDELT_process")
        .getOrCreate()

    val url = "jdbc:mysql://ec2-54-202-161-91.us-west-2.compute.amazonaws.com:3306/gdeltDB"
    val driver = "com.mysql.cj.jdbc.Driver"
    val username = "sparkMS"
    val password = "test"

    try {
      Class.forName(driver)
      val dbConnection = DriverManager.getConnection(url, username, password)
      process_events(dbConnection, spark)
      process_mentions(dbConnection, spark)


      dbConnection.close
    } catch {
      case e: Exception => e.printStackTrace
    }

    // val fileName = "v2/events/20150218230000.export.csv"


    // val linesRdd = sc.textFile("s3://insightdatarichard/"+ fileName)
    // val linesMap = linesRdd.map( line => line.split("\t") )
    // val lineRdd = linesMap.map( fields => {
    //   val eventID = fields(0)
    //   val date = fields(1)
    // } )
    //collect() fetches the entire RDD to a single machine
    // lineRdd.foreach( { line => println( s"{ $eventID, $date }" ) } )

    spark.stop()
  }
}