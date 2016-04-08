import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.util
import org.apache.spark.sql.SQLContext

object OriginalvsRetweets {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\Users\\kisho\\Documents\\hadoop-common-2.2.0-bin-master")
    val config = new SparkConf().setAppName("CountSpark").setMaster("local[2]").set("spark.executor.memory", "8g")
    val sparkContext = new SparkContext(config)
    val sqlContext = new SQLContext(sparkContext)
    val inputFile = sqlContext.jsonFile("C:\\Users\\kisho\\Documents\\\\PythonTweetsPanamaPapers.json")
    inputFile.registerTempTable("querytable1")
    val original_query = sqlContext.sql("select count(id) as retweetscount from querytable1 " +
      "where retweeted_status is not null union select  count(id) as retweetscount from querytable1 where retweeted_status is null")
    //original_query.save("OriginalvsRetweets","json")
    original_query.show()
  }
}