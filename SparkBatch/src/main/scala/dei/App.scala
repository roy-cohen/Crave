package dei

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.cassandra._

/**
  * Created by roycohen on 6/25/16.
  */
object App {

  //val conf = new SparkConf().setAppName("yelp batch job").setMaster("local[*]")
  //val sc = new SparkContext(conf)
  val conf = new SparkConf(true).
    set("spark.cassandra.connection.host", "127.0.0.1").
    setAppName("yelp batch job").
    setMaster("local[*]")
  val sc = new SparkContext(conf)
  //val rdd = sc.cassandraTable("dishes_db", "dishes")
  //rdd.count
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  def main(args : Array[String]): Unit = {

    println("hello world");

    val categoryItems = collection.mutable.Map[String, collection.mutable.Set[String]]()
    val items = collection.mutable.Set[String]()

    val itemsInstance = new ItemsAndCategories()
    itemsInstance.readCategoriesAndItems("input/dishes.txt", categoryItems, items)


    //val sample = sqlContext.read.json("input/yelp_academic_dataset_review.json")
    //val sample = sqlContext.read.json("s3://roy-data/yelp_reviews.json")


    val reviewsInstance = new Reviews()
    val tipsInstance = new Tips()
    val businessInstance = new Business()

    val scores = reviewsInstance.getScores(reviewsInstance.reviews, items)
    val tipScores = tipsInstance.getScores(tipsInstance.tips, items)
    val union = scores.union(tipScores)

    val rowRDD = scores.map(p => Row(p._1, p._2, p._3, p._4, p._5, p._6, p._7) )
    val scoreSchema = StructType(Array(StructField("businessid",StringType,true),StructField("dish",StringType,true),StructField("avgrating",DoubleType,true),StructField("totalscore",IntegerType,true),StructField("numreviews",IntegerType,true),StructField("promotext",StringType,true),StructField("promoscore",IntegerType,true)))
    val scoresDF = sqlContext.createDataFrame(rowRDD, scoreSchema)
    scoresDF.registerTempTable("scores")

    val joined = scoresDF.join(businessInstance.businesses, scoresDF("businessid") === businessInstance.businesses("business_id")).
      drop(businessInstance.businesses("business_id")).cache

    val rtmp = joined.select(joined("businessid"), joined("city"), joined("state"), joined("dish"), joined("avgrating"), joined("totalscore"), joined("numreviews"), joined("promotext"), joined("promoscore") )
    val rtmp2 = rtmp.map(row => (row(0),row(1),row(2),row(3),row(4),row(5),row(6),row(7),row(8)))
    val rtmp3 = rtmp2.map{case (businessid, city, state, dish, avgrating, totalscore, numreviews, promotext, promoscore) => (businessid.toString, dish.toString, java.lang.Double.parseDouble(avgrating.toString), java.lang.Integer.parseInt(totalscore.toString), java.lang.Integer.parseInt(numreviews.toString), promotext.toString, java.lang.Integer.parseInt(promoscore.toString), city.toString + state.toString)}
    val reviewRowRDD = rtmp3.map(p => Row(p._1, p._2, p._3, p._4, p._5, p._6, p._7, p._8))
    val reviewSchema = StructType(Array(StructField("businessid",StringType,true),StructField("dish",StringType,true),StructField("avgrating",DoubleType,true),StructField("totalscore",IntegerType,true),StructField("numreviews",IntegerType,true),StructField("promotext",StringType,true),StructField("promoscore",IntegerType,true),StructField("citystate",StringType,true)))
    val reviewsToStore = sqlContext.createDataFrame(reviewRowRDD, reviewSchema)
    //val reviewsToStore = joined.select(joined("businessid"), joined("city"), joined("dish"), joined("avgrating"), joined("totalscore"), joined("numreviews"), joined("promotext"), joined("promoscore") )
    reviewsToStore.write.cassandraFormat("dishes", "dishes_db").save()

    val tmp = joined.select(joined("businessid"), joined("name"), joined("full_address"), joined("city"), joined("state"), joined("stars"), joined("review_count"))
    val tmp2 = tmp.map(row => (row(0),row(1),row(2),row(3),row(4),row(5),row(6)))
    val tmp3 = tmp2.map{case (businessid, name, full_address, city, state, stars, reviewcount) => (businessid.toString, name.toString, full_address.toString, city.toString, state.toString, java.lang.Double.parseDouble(stars.toString), java.lang.Integer.parseInt(reviewcount.toString), city.toString + state.toString)}
    val businessRowRDD = tmp3.map(p => Row(p._1, p._2, p._3, p._4, p._5, p._6, p._7, p._8) )
    //val businessesToStore = joined.select(joined("businessid"), joined("name"), joined("full_address"), joined("city"), joined("state"))
    val businessSchema = StructType(Array(StructField("businessid",StringType,true),StructField("name",StringType,true),StructField("full_address",StringType,true),StructField("city",StringType,true),StructField("state",StringType,true),StructField("stars",DoubleType,true),StructField("reviewcount",IntegerType,true),StructField("citystate",StringType,true)))
    val businessesToStore = sqlContext.createDataFrame(businessRowRDD, businessSchema)
    businessesToStore.write.cassandraFormat("businesses", "dishes_db").save()

  }
}
