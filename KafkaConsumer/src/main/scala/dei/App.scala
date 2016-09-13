package dei

import java.io.Serializable

import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf}
import com.datastax.spark.connector.cql.CassandraConnector

import scala.collection.JavaConversions._
import com.datastax.spark.connector.writer._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.concurrent.duration.DurationInt


object App {

  case class Review(num: Int, dish: String, id: String, citystate: String, score: Int, promotext : String, avg : java.lang.Double, weightedScore : java.lang.Double)

  def getWeightedScore (numreviews : Int, avgrating: Int) : java.lang.Double = {

    val weight = numreviews match {
      case x if (x >= 0 && x <= 4) => 0.4;
      case x if (x == 5) => 0.5;
      case x if (x == 6) => 0.6;
      case x if (x == 7) => 0.7;
      case x if (x == 8) => 0.8;
      case x if (x == 9) => 0.9;
      case x if (x >= 10) => 1.0;
    }

    val weightedScore : java.lang.Double = avgrating * weight

    weightedScore
  }

  def main(args:Array[String]) = {

    val log = new MyLogger()
    Logger.getRootLogger().setLevel(Level.WARN)
    // Create Spark context.
    val conf = new SparkConf().
      set("spark.cassandra.connection.host", "127.0.0.1").
      setAppName("KafkaConsumer").
      setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))
    //ssc.checkpoint("checkpoint")
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


    val windowDuration = Seconds(new DurationInt(14).days.toSeconds)
    val slideDuration = Seconds(new DurationInt(1).days.toSeconds)

    val reviews : DStream[(String, Review)] = kafkaStream.map{case(key, values) => {
      val tokens = values.split(",")
      //log.warn("newreview: " + values)

      if (tokens.size > 4) {

        (new String(key), new Review(1, tokens(0), tokens(1), tokens(2), java.lang.Integer.parseInt(tokens(3)), tokens(4), java.lang.Double.parseDouble(tokens(3)), 0.0))
      }else {

        (new String(key), new Review(1, tokens(0), tokens(1), tokens(2), java.lang.Integer.parseInt(tokens(3)), "", java.lang.Double.parseDouble(tokens(3)), 0.0))
      }

    }}

    val aggregegate : DStream[(String, Review)] = reviews.reduceByKeyAndWindow(
        (a:Review,b:Review) => new Review(a.num + b.num, a.dish, a.id, a.citystate, a.score + b.score, a.promotext, (a.score + b.score) * 1.0 / (a.num + b.num), getWeightedScore(a.num + b.num, a.score + b.score)),
        windowDuration,
        slideDuration)


    //update dishes table with new ratings
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

            //update dishes table with new values
            val applied = session.execute("UPDATE dishes_db.dishes set avgrating = ?, numreviews = ?, totalscore = ? WHERE citystate = ? AND businessid = ? AND dish = ? if exists", newAverage, newNumReviews, newTotalScore, review.citystate, review.id, review.dish).wasApplied()

//            if (applied) log.warn("success") else log.warn("fail")
          }
        }
        session.close()
      }}


      val groups : Map[String, Array[(String, Review)]] = rdd.collect().groupBy{ case(key, review) => review.citystate}

      //var index = 0;
      groups.foreach(rdd => {
        //val key : String = rdd._1
        //log.warn(index.toString + " " + key)
        //index = index + 1

        val reviews : Array[(String, Review)] = rdd._2
        val sorted = reviews.map{case(citystate, review) => review}.sortBy(review => -review.weightedScore)
        val top3 : Array[Review] = sorted.take(3)

        val session = connector.openSession()
        session.execute("DELETE FROM dishes_db.trends WHERE citystate = ?", top3(0).citystate)

        for(i <- 0 to top3.length - 1){
          val review = top3(i)

          //log.warn(review.promotext)

//          val insertStatement = "INSERT INTO dishes_db.trends (citystate, avgrating, businessId, dish, numreviews, totalscore, promotext) VALUES (" + review.citystate + ", " + review.avg + ", " + review.id + ", " + review.dish + ", " + new Integer(review.num) + ", " + new Integer(review.score) + ", " + review.promotext + ")"
//          log.warn(insertStatement)

          session.execute("INSERT INTO dishes_db.trends (citystate, avgrating, businessId, dish, numreviews, totalscore, promotext) VALUES (?,?,?,?,?,?,?)", review.citystate, review.avg, review.id, review.dish, new Integer(review.num), new Integer(review.score), review.promotext)
        }

        session.close()
      })

    })



    // Close session
//    session.close


    ssc.start()
    ssc.awaitTermination()
  }

}
