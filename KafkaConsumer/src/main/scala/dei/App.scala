package dei

import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf}
import com.datastax.spark.connector.cql.CassandraConnector
import scala.collection.JavaConversions._
import com.datastax.spark.connector.writer._


object App {

  object Util extends Logging {
    def setStreamingLogLevels() {
      // Set reasonable logging levels for streaming if the user has not configured log4j.
      val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
      if (!log4jInitialized) {
        // Force initialization.
        logInfo("Setting log level to [WARN] for streaming example." +
          " To override add a custom log4j.properties to the classpath.")
        Logger.getRootLogger.setLevel(Level.WARN)
      }
    }
  }

  case class Review(num: Int, dish: String, id: String, citystate: String, score: Int)

  def main(args:Array[String]) = {
    // Disable chatty logging.
    Util.setStreamingLogLevels()

    // Create Spark context.
    val conf = new SparkConf().
      set("spark.cassandra.connection.host", "127.0.0.1").
      setAppName("KafkaConsumer").
      setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("checkpoint")
    val sc = ssc.sparkContext
    val connector = CassandraConnector(sc.getConf)

    // Brokers.
    val brokers = Set("localhost:9092")
    val kafkaParams = Map("metadata.broker.list" -> brokers.mkString(","))

    // Topics.
    val topics = Set("newreview")

    // Create DStream.
    val kafkaStream = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](
      ssc, kafkaParams, topics)


    val windowDuration = Seconds(10)
    val slideDuration = Seconds(10)

    val reviews = kafkaStream.map{case(key, values) => {
      val tokens = values.split(",")
      (new String(key), new Review(1, tokens(0), tokens(1), tokens(2), java.lang.Integer.parseInt(tokens(3)) ))
    }}

    val aggregegate = reviews.reduceByKeyAndWindow(
        (a:Review,b:Review) => new Review(a.num + b.num, a.dish, a.id, a.citystate, a.score + b.score),
        windowDuration,
        slideDuration)

    //Open session
    val connectorBroadcast = sc.broadcast(connector)//.openSession
    //val sessionBroadcast = sc.broadcast(session)


    aggregegate.foreachRDD(rdd => {

      rdd.foreachPartition{ iter: Iterator[(String,Review)] => {

        val session = connector.openSession()

        while (iter.hasNext)
        {
          val review = iter.next()._2

          //query dishes table by business, citystate, and dish

          val reviews = session.execute("SELECT citystate, dish, businessid, avgrating, numreviews, totalscore FROM dishes_db.dishes WHERE citystate = ? AND businessid = ? AND dish = ?", review.citystate, review.id, review.dish)
          val reviewList = reviews.toList
          //this is based off random simulator, and I only want to update rows that exists
          if (reviewList.length > 0) {
            val row = reviewList.get(0)

            //add the current rdd's values to it
            val newNumReviews: java.lang.Integer = row.getInt("numreviews") + review.num;
            val newTotalScore: java.lang.Integer = row.getInt("totalscore") + review.score;
            val newAverage: java.lang.Double = (newTotalScore * 1.0) / newNumReviews;
//            can't print, log instead
//            println("before " + review.id + " num reviews=" + row.getInt("numreviews") + " total score=" + row.getInt("totalscore") + "average= " + row.getDouble("avgrating"))
//            println("after " + review.id + " num reviews=" + newNumReviews + " total score=" + newTotalScore + " average= " + newAverage.toString)
            //update dishes table with new values
            val applied = session.execute("UPDATE dishes_db.dishes set avgrating = ?, numreviews = ?, totalscore = ? WHERE citystate = ? AND businessid = ? AND dish = ? if exists", newAverage, newNumReviews, newTotalScore, review.citystate, review.id, review.dish).wasApplied()

//            can't print, log instead
//            if (applied) println("success") else println("fail")

          }
        }
        session.close()
      }}

    })

    // Close session
//    session.close


    ssc.start()
    ssc.awaitTermination()
  }

}
