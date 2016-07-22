package dei

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra._
import org.apache.spark.storage.StorageLevel


object App {

  private var sc: SparkContext = _
  private var sqlContext: SQLContext = _

  def main(args : Array[String]): Unit = {

    //val logger = Logger.getLogger(App.getClass)

    //val conf = new SparkConf().setAppName("yelp batch job").setMaster("local[*]")
    //val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val conf = sc.getConf
    conf.registerKryoClasses(Array(classOf[Reviews], classOf[Business]))

    val categoryItems = collection.mutable.Map[String, collection.mutable.Set[String]]()
    val items = collection.mutable.Set[String]()

    val itemsInstance = new ItemsAndCategories(sc)
    itemsInstance.readCategoriesAndItems(categoryItems, items)

    val reviewsInstance = new Reviews(sqlContext)
    val businessInstance = new Business(sqlContext)

    val reviewScores = Processor.getScores(reviewsInstance.reviews, items)
    //reviewScores.saveAsObjectFile("s3://roy-write/reviewScores.obj")

    val tipScores = Processor.getScores(reviewsInstance.tips, items)
    //tipScores.saveAsObjectFile("s3://roy-write/tipScores.obj")

    val union = reviewScores.union(tipScores)

    val averageScores : RDD[Row] = Processor.getAverageScores(union)
    //averageScores.saveAsObjectFile("s3://roy-write/averageScores.obj")

    val scoresDF = getScoresDF(averageScores)

    val joined : DataFrame = scoresDF.join(businessInstance.businesses, scoresDF("businessid") === businessInstance.businesses("business_id")).
      drop(businessInstance.businesses("business_id"))

    saveDishes(joined)
    saveBusinesses(joined)
  }

  def getScoresDF(averageScores: RDD[Row]) : DataFrame = {
    val scoreSchema = StructType(Array(StructField("businessid",StringType,true),StructField("dish",StringType,true),StructField("avgrating",DoubleType,true),StructField("totalscore",IntegerType,true),StructField("numreviews",IntegerType,true),StructField("promotext",StringType,true),StructField("promoscore",IntegerType,true)))
    val scoresDF = sqlContext.createDataFrame(averageScores, scoreSchema)
    scoresDF
  }

  def saveDishes(joined: DataFrame) = {
    val rtmp1 : DataFrame = joined.select(joined("businessid"), joined("city"), joined("state"), joined("dish"), joined("avgrating"), joined("totalscore"), joined("numreviews"), joined("promotext"), joined("promoscore") )
    val rtmp2 = rtmp1.map(row => (row(0),row(1),row(2),row(3),row(4),row(5),row(6),row(7),row(8)))
    val rtmp3 = rtmp2.map{case (businessid, city, state, dish, avgrating, totalscore, numreviews, promotext, promoscore) => (businessid.toString, dish.toString, java.lang.Double.parseDouble(avgrating.toString), java.lang.Integer.parseInt(totalscore.toString), java.lang.Integer.parseInt(numreviews.toString), promotext.toString, java.lang.Integer.parseInt(promoscore.toString), city.toString + state.toString)}
    val dishRowRDD = rtmp3.map(p => Row(p._1, p._2, p._3, p._4, p._5, p._6, p._7, p._8))
    val dishSchema = StructType(Array(StructField("businessid",StringType,true),StructField("dish",StringType,true),StructField("avgrating",DoubleType,true),StructField("totalscore",IntegerType,true),StructField("numreviews",IntegerType,true),StructField("promotext",StringType,true),StructField("promoscore",IntegerType,true),StructField("citystate",StringType,true)))
    val dishesToStore = sqlContext.createDataFrame(dishRowRDD, dishSchema)
    dishesToStore.write.cassandraFormat("dishes", "dishes_db").save()
  }

  def saveBusinesses(joined: DataFrame) = {
    val btmp1 = joined.select(joined("businessid"), joined("name"), joined("full_address"), joined("city"), joined("state"), joined("stars"), joined("review_count"))
    val btmp2 = btmp1.map(row => (row(0),row(1),row(2),row(3),row(4),row(5),row(6)))
    val btmp3 = btmp2.map{case (businessid, name, full_address, city, state, stars, reviewcount) => (businessid.toString, name.toString, full_address.toString, city.toString, state.toString, java.lang.Double.parseDouble(stars.toString), java.lang.Integer.parseInt(reviewcount.toString), city.toString + state.toString)}
    val businessRowRDD = btmp3.map(p => Row(p._1, p._2, p._3, p._4, p._5, p._6, p._7, p._8) )
    val businessSchema = StructType(Array(StructField("businessid",StringType,true),StructField("name",StringType,true),StructField("full_address",StringType,true),StructField("city",StringType,true),StructField("state",StringType,true),StructField("stars",DoubleType,true),StructField("reviewcount",IntegerType,true),StructField("citystate",StringType,true)))
    val businessesToStore = sqlContext.createDataFrame(businessRowRDD, businessSchema)
    businessesToStore.write.cassandraFormat("businesses", "dishes_db").save()
  }
}
