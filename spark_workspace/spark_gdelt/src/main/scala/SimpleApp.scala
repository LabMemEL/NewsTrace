// package gdelt.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import scala.io.Source
import java.util.Properties
// import com.amazonaws.services.s3.AmazonS3Client
// import com.amazonaws.services.s3.model.GetObjectRequest
import org.apache.spark.{SparkContext, SparkConf}
import java.sql.{DriverManager, Connection}

object SimpleApp {

  def create_table(connection: Connection, tablename: String){

    // val driver =
    // val jdbcDF = SQLContext.load("jdbc", Map(
    //   "url" -> "jdbc:postgresql://ec2-54-202-161-91.us-west-2.compute.amazonaws.com/post"
    // ))
    // var connection:Connection = _
      val statement = connection.createStatement
      val update = "CREATE TABLE IF NOT EXISTS " + tablename + """ (
                    EID INT PRIMARY KEY,
                    Date TEXT,
                    act1 TEXT,
                    act2 TEXT,
                    URL TEXT NOT NULL
                    )  ENGINE=INNODB;"""
      val rs = statement.executeUpdate(update)
  }

  def updateDB(records_df: DataFrame, tablename: String) {  //update(records_df: DataFrame)

    // val sparkSes_db = SparkSession
    //     .builder()
    //     .appName("GDELT_updateDB")
    //     .getOrCreate()

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
    // var connection:Connection = _ls
    // try {
    // } catch {
    //   case e: Exception => e.printStackTrace
    // }
    // sparkSes_db.stop()
  }


  // }

  def main(args: Array[String]) {

    val fileNames = Source.fromFile("eventlist.out").getLines.toList.take(100)
    
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
      val connection = DriverManager.getConnection(url, username, password)


    // ideally start of parallel work
    // val lookup = Map("_c0" -> "EID", "_c1" -> "Date", "_c44" -> "act1", "_c52" -> "act2", "_c60" -> "URL")
      val names = Seq("EID", "Date", "act1", "act2", "URL")
      for (fileName <- fileNames) {
        // v2/events/20150715014500.export.csv into 2015_07_15
        val tablename = "TB_" + fileName.split("/")(2).substring(0,8)
        create_table(connection, tablename)

        val full_df = spark.read.format("csv")
            .option("sep", "\t")
            // .option("header", "true") //first line in file has headers
            .option("mode", "DROPMALFORMED")
            .load("s3a://insightdatarichard/"+ fileName) //s3 triggers jets3t exception
        // gdelt_full_df.show()

        val main_DF  = full_df.select("_c0","_c1","_c44","_c52","_c60")
        val renamed_df = main_DF.toDF(names: _*)

        // print(renamed_df.show(1))
        updateDB(renamed_df, tablename)
      }

      // rs is the result set object, reads with a cursor (line by line)
      connection.close
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