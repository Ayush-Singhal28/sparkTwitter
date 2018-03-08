package edu.knoldus

import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder



object Application {
  val conf = new SparkConf().setMaster("local[4]").setAppName("Spark Streaming - PopularHashTags")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {


    val config = ConfigFactory.load("application.conf")
    val consumerKey = config.getString("Twitter.key.consumerKey")
    val consumerSecret = config.getString("Twitter.key.consumerSecret")
    val accessToken = config.getString("Twitter.key.accessToken")
    val accessTokenSecret = config.getString("Twitter.key.accessTokenSecret")
    val logger = Logger.getLogger(this.getClass)


    val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().setDebugEnabled(true)
      .setDebugEnabled(false)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
       .build()))
    val JDBC_DRIVER = "com.mysql.jdbc.Driver"

    val USER = "root"
    val PASS = "qwerty"
    val DB_URL = "jdbc:mysql://localhost/dbTwitter"



    import java.sql.DriverManager
    Class.forName("com.mysql.jdbc.Driver")


    logger.info("Connecting to database...")
    val conn = DriverManager.getConnection(DB_URL, USER, PASS)


    System.out.println("Creating statement...")
    val stmt = conn.createStatement

    val ssc = new StreamingContext(sc, Seconds(5))
    val stream = TwitterUtils.createStream(ssc, auth)
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    val topCounts: DStream[(Int, String)] = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(20))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))
    topCounts.foreachRDD(rdd => {
      val topThreeList = rdd.take(3)
      logger.info("\nTop three hashTags in last 20 seconds (%s total):\n".format(rdd.count()))
      topThreeList.foreach { case (count, tag) =>
        logger.info("%s (%s tweets)\n".format(tag, count))
        val query = " insert into twitter (hashTag, count)" + " values (?, ?)"
        val preparedStmt = conn.prepareStatement(query)
        preparedStmt.setString(1, tag)
        preparedStmt.setInt(2, count)
        preparedStmt.execute
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}


