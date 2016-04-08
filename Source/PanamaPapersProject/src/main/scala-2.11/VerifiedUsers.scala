import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.util
import org.apache.spark.sql.SQLContext

object VerifiedUsers {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\Users\\kisho\\Documents\\hadoop-common-2.2.0-bin-master")
    val config = new SparkConf().setAppName("CountSpark").setMaster("local[2]").set("spark.executor.memory", "8g")
    val SparkContext = new SparkContext(config)
    val sqlContext = new SQLContext(SparkContext)
    val inputFile = sqlContext.jsonFile("C:\\Users\\kisho\\Documents\\PythonTweetsPanamaPapers.json")
    inputFile.registerTempTable("querytable1")
    val verified_query = sqlContext.sql("select user.verified, count(id) as CountOfUsers from querytable1 " +
      "WHERE user.verified is not null group by user.verified")
    verified_query.save("VerifiedUsers","json")


  }
}