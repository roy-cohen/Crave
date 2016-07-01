package dei



//import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkContext
//import org.apache.spark.sql.SQLContext._
//import org.apache.spark.SparkConf
import scala.collection.JavaConversions._
import java.util.HashMap
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.kafka._

/**
  * Created by roycohen on 6/29/16.
  */

object App {

  val conf = new SparkConf(true).
    set("spark.cassandra.connection.host", "127.0.0.1").
    setMaster("local[*]").
    setAppName("KafkaProducer")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val connector = CassandraConnector(sc.getConf)

  def main(args : Array[String]): Unit = {

    //    1. read dishes
    val dishes = readCategoriesAndItems("input/dishes.txt").toArray

    //    2. read businesses
    val businesses = getBusinesses().toArray


    val brokers = "localhost:9092"
    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    while(true) {

        //    3. produce random item, business, score
        val newreview = getNewReview(dishes, businesses)
        val tokens = newreview.split(",")
        val key =  tokens(0) + tokens(1)

        //    4. send review to kafka
        val message = new ProducerRecord[String, String]("newreview", key, newreview)
        producer.send(message)
        Thread.sleep(1000)
      }
  }

  def readCategoriesAndItems(fileName: String) : collection.mutable.Set[String] = {

    val dishes = collection.mutable.Set[String]()
    val source = scala.io.Source.fromFile(fileName);

    for (line <- source.getLines) {

      val tokens = line.split(":");
      val categoryLine = tokens(0);
      val dish = tokens(1);

      dishes.add(dish)
    }

    dishes
  }

  def getBusinesses() : scala.collection.mutable.Buffer[String] = {

    val session = connector.openSession
    val resultSet = session.execute("SELECT businessid, citystate FROM dishes_db.businesses")
    val rows = resultSet.all

    // Close session
    session.close

    val businessids = rows.map(row => row.getString("businessid") + "," + row.getString("citystate"))
    businessids
  }

  def getNewReview(dishes: Array[String], businesses: Array[String]) = {
    val r = scala.util.Random
    var index = r.nextInt(dishes.length)
    val dish = dishes(index)

    index = r.nextInt(businesses.length)
    val business = businesses(index)

    val score = r.nextInt(5) + 1

    val review = dish + "," + business + "," + score.toString
    review
  }
}
