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
  val conf = new SparkConf(true).set("spark.cassandra.connection.host", "172.31.62.42")
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
    val schema = StructType(Array(StructField("businessid",StringType,true),StructField("dish",StringType,true),StructField("avgrating",DoubleType,true),StructField("totalscore",IntegerType,true),StructField("numreviews",IntegerType,true),StructField("promotext",StringType,true),StructField("promoscore",IntegerType,true)))
    val scores = reviewsInstance.getScores(reviewsInstance.reviews, items)
    val rowRDD = scores.map(p => Row(p._1, p._2, p._3, p._4, p._5, p._6, p._7) )
    val scoresDF = sqlContext.createDataFrame(rowRDD, schema)
    scoresDF.registerTempTable("scores")

    val businessInstance = new Business()

    val joined = scoresDF.join(businessInstance.businesses, scoresDF("businessid") === businessInstance.businesses("business_id")).drop(businessInstance.businesses("business_id")).cache
    //val reviewsToStore = joined.map(row =>
    //(joined("city"), joined("businessId"), joined("dish"), joined("avgrating"), joined("totalScore"), joined("numreviews"), joined("promoText"), joined("promoScore"))
    //)

    val reviewsToStore = joined.select(joined("businessid"), joined("city"), joined("dish"), joined("avgrating"), joined("totalscore"), joined("numreviews"), joined("promotext"), joined("promoscore") )
    reviewsToStore.write.cassandraFormat("dishes", "dishes_db").save()
    //val reviewsToStore = joined.map(row =>
    //    (joined("city"), joined("businessId"), joined("dish"), joined("averageRating"), joined("totalScore"), joined("numreviews"), joined("promoText"), joined("promoScore"))
    //    )

    //val businessesToStore = joined.select(joined("businessId"), joined("name"), joined("full_address"), joined("city"), joined("state"))


    //val test = scores.filter{ case(id, item, totalscore, numreviews, avgscore) => numreviews > 1}.take(20)
    //test.foreach(println)
    //  filteredScores.map{ case(id, item, score, review) => ((item + id), 1}.reduceByKey(_+_)

    // Write to Cassandra
    //val saveData = scores.takeSample(false, 20)
    //reviewsToStore.saveToCassandra("dishes_db","dishes",
    //  SomeColumns("city", "businessid", "dish", "avgrating", "totalscore", "numreviews", "promotext", "promoscore"))

    //businessesToStore.saveToCassandra("dishes_db","businesses",
    //  SomeColumns("businessid", "name", "full_address", "city", "state"))

    //ColumnDef(promo,RegularColumn,VarCharType), ColumnDef(promorating,RegularColumn,IntType)


  }
}
