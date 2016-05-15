import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
object TweetsEachHour {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\Users\\kisho\\Documents\\hadoop-common-2.2.0-bin-master")
    val config = new SparkConf().setAppName("CountSpark").setMaster("local[2]").set("spark.executor.memory", "8g")
    val sparkcontext = new SparkContext(config)
    val sqlContext = new SQLContext(sparkcontext)
    val tweetsfile = sqlContext.jsonFile("C:\\\\Users\\\\kisho\\\\Documents\\\\PythonTweetsPanamaPapers.json")
    tweetsfile.registerTempTable("tweettable")
    val hour_query = sqlContext.sql("select SUBSTR(created_at,12,2) as Hour,count(id) as TweetsEachHour from tweettable group by SUBSTR(created_at,12,2) " +
      "order by count(id) desc limit 5")
    //hour_query.save("HourlyTweets","json")
    hour_query.show()
  }
}
