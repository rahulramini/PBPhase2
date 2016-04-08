import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.util
import org.apache.spark.sql.SQLContext
object SnowdenSpread {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\Users\\kisho\\Documents\\hadoop-common-2.2.0-bin-master")
    val config = new SparkConf().setAppName("CountSpark").setMaster("local[2]").set("spark.executor.memory", "8g")
    val sparkContext = new SparkContext(config)
    val sqlContext = new SQLContext(sparkContext)
    val inputFile = sqlContext.jsonFile("C:\\\\Users\\\\kisho\\\\Documents\\\\PythonTweetsPanamaPapers.json")
    inputFile.registerTempTable("querytable1")
    val snowden_query = sqlContext.sql("select user.time_zone,count(id) as TopMentions from querytable1" +
      " where entities.user_mentions.name like '%Snowden%' group by user.time_zone order by TopMentions desc limit 20")
    snowden_query.save("SnowdenSpread","json")

  }
}