import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import scala.io.Source
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.GetObjectRequest


object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/usr/local/spark/README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    println("hello?")


    println("sample record")
    val s3Client = new AmazonS3Client()
    val s3Object = s3Client.getObject("insightdatarichard", "v2/events/20150218230000.export.csv")
    val myData = Source.fromInputStream(s3Object.getObjectContent()).getLines()  //buffered sources
    val line = myData.next()
    print(line + "\nGDELT record retrived above\n") //this is exactly one row in the csv

    // val driver =
    // val jdbcDF = SQLContext.load("jdbc", Map(
    //   "url" -> "jdbc:postgresql://ec2-54-202-161-91.us-west-2.compute.amazonaws.com/post"
    // ))
    import java.sql.{DriverManager, Connection}
    val url = "jdbc:mysql://ec2-54-202-161-91.us-west-2.compute.amazonaws.com:3306/gdeltDB"
    val driver = "com.mysql.cj.jdbc.Driver"
    val username = "sparkMS"
    val password = "test"
    // var connection:Connection = _
    try {
      Class.forName(driver)
      val connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement
      val update = """CREATE TABLE IF NOT EXISTS tasks (
      task_id INT AUTO_INCREMENT PRIMARY KEY,
      title VARCHAR(255) NOT NULL,
      start_date DATE,
      due_date DATE,
      status TINYINT NOT NULL,
      priority TINYINT NOT NULL,
      description TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )  ENGINE=INNODB;"""
      val rs = statement.executeUpdate(update)
      // rs is the result set object, reads with a cursor (line by line)
      
      val query = """select * from tasks;"""
      val rs_query = statement.executeQuery(query)
      val rsmd = rs_query.getMetaData();
      println("No. of columns : " + rsmd.getColumnCount());
      println("Column name of 1st column : " + rsmd.getColumnName(1));
      println("Column type of 1st column : " + rsmd.getColumnTypeName(1));


      connection.close
    } catch {
      case e: Exception => e.printStackTrace
    }

    spark.stop()
  }
}