import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.util
import org.apache.spark.sql.SQLContext

object FollowersCount {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\Users\\kisho\\Documents\\hadoop-common-2.2.0-bin-master")
    val config = new SparkConf().setAppName("CountSpark").setMaster("local[2]").set("spark.executor.memory","8g")
    val sparkContext = new SparkContext(config)

    val sqlContext = new SQLContext(sparkContext)
    val inputFile = sqlContext.jsonFile("C:\\\\Users\\\\kisho\\\\Documents\\\\PythonTweetsPanamaPapers.json")
    inputFile.registerTempTable("querytable1")
    val followers_query = sqlContext.sql("select user.name, max(user.followers_count) as followerscount from querytable1" +
      " group by user.name order by followerscount desc limit 10")
    //followers_query.save("followersCount","json")
      followers_query.show()
  }
}