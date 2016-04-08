import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.util
import org.apache.spark.sql.SQLContext

object LanguageSpread {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\Users\\kisho\\Documents\\hadoop-common-2.2.0-bin-master")
    val config = new SparkConf().setAppName("CountSpark").setMaster("local[2]").set("spark.executor.memory","8g")
    val sc = new SparkContext(config)

    val sqlContext = new SQLContext(sc)
    val tweetsfile = sqlContext.jsonFile("C:\\Users\\kisho\\Documents\\PythonTweetsPanamaPapers.json")
    tweetsfile.registerTempTable("querytable1")
    val query1 = sqlContext.sql("select user.lang,count(id) as language from querytable1 group by user.lang " +
      "order by language desc limit 20")

    query1.save("LanguageSpread","json")

  }
}