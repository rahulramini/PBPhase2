import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.util
import org.apache.spark.sql.SQLContext

/**
  * Created by kisho on 4/5/2016.
  */
object PopularRetweets {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\Users\\kisho\\Documents\\hadoop-common-2.2.0-bin-master")
    val config = new SparkConf().setAppName("CountSpark").setMaster("local[2]").set("spark.executor.memory", "8g")
    val sparkContext = new SparkContext(config)


    val sqlContext = new SQLContext(sparkContext)
    val inputfile = sqlContext.jsonFile("C:\\Users\\kisho\\Documents\\PythonTweetsPanamaPapers.json")
    inputfile.registerTempTable("querytable1")
    val retweets_query = sqlContext.sql("select retweeted_status.user.screen_name as User," +
      " retweeted_status.id as TweetID, max(retweeted_status.retweet_count) as RetweetCount from querytable1" +
      " group by retweeted_status.id, retweeted_status.user.screen_name order by RetweetCount desc limit 10")
    retweets_query.save("Popular Retweets","json")

  }
}