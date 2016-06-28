package dei

import org.apache.spark
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.{DefaultFormats, Formats, JValue}
import org.scalatra.ScalatraServlet
import org.scalatra.json.JacksonJsonSupport
import com.datastax.spark.connector._
import org.apache.spark.sql.{DataFrame, DataFrameHolder, Row}
import org.apache.spark.sql.types._



class CraveServlet extends ScalatraServlet with JacksonJsonSupport {
  protected implicit val jsonFormats: Formats = DefaultFormats
  case class DataPoint(businessid: String, avgrating: Double, dishnumreviews: Integer, promotext:String, address:String, name:String, restaurantnumreviews:Int, stars:Double) extends java.io.Serializable
  //case class MyRow(key: String, value: Long)

  val conf = new SparkConf(true).
    set("spark.cassandra.connection.host", "127.0.0.1").
    setMaster("local[*]").
    setAppName("Crave")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  //val dishesRDD = sc.cassandraTable("dishes_db", "dishes").cache
  //val businessRDD = sc.cassandraTable("dishes_db", "businesses").cache

  val categoryfile = System.getProperty("user.dir") + "/static/categories.txt";
  val categoryItems = getCategories(categoryfile);

  ///Users/roycohen/capstone/DishRecommender/SparkWeb/static/categories.txt



  before() {
    //contentType = "text/json"
    contentType = formats("json")
    //contentType = "application/json"

  }

  get ("/") {

  }

  get("/dishes.json") {
    val file = System.getProperty("user.dir") + "/static/dishes.json"
    val dishes = scala.io.Source.fromFile(file).getLines.mkString
    dishes
  }

  get("/cities.json") {
    val file = System.getProperty("user.dir") + "/static/cities.json"
    val cities = scala.io.Source.fromFile(file).getLines.mkString
    cities
  }

  get("/searchDishes.json") {

    val citystate = params.get("city").get.toString.replaceAll(",","").replaceAll(" ","")
    val dish = params.get("dish").get.toString
    //var results = Array(com.datastax.spark.connector.rdd.CassandraTableScanRDD)


    if (categoryItems.contains(dish)){
//      //this item is a category, so do multiple dish search
      //val dishes = categoryItems(dish).toList
      val dishes = categoryItems(dish).mkString("'", "','", "'")
//      val results = dishesRDD.select("businessid","avgrating","numreviews","promoscore", "promotext", "totalscore").
//        where("citystate = ? AND dish IN ?",citystate, dishes)
//
//      val topreviews = results.sortBy(row => -row.getDouble("avgrating")).take(5).map(row => {
//
//        val id = row.getString("businessid")
//        val avgrating = row.getDouble("avgrating")
//        val numreviews = row.getInt("numreviews")
//        val promoscore = row.getInt("promoscore")
//        val promotext = row.getString("promotext")
//        val totalscore = row.getInt("totalscore")
//
//        (id,avgrating,numreviews,promoscore,promotext,totalscore)
//
//      }).toList

//      val businessIds = topreviews.map(x => x.businessid).toList
//      val businesses = businessRDD.select("businessid","full_address","name","reviewcount", "stars").
//      where("citystate = ? AND businessid IN ?", citystate, businessIds)
//      join business info with review on business id and return to client


      //topreviews
      val dishesDF = sqlContext.
        read.
        format("org.apache.spark.sql.cassandra").
        options(Map("table" -> "dishes", "keyspace" -> "dishes_db")).
        load()
      dishesDF.registerTempTable("dishes")

      val businessDF = sqlContext.
        read.
        format("org.apache.spark.sql.cassandra").
        options(Map("table" -> "businesses", "keyspace" -> "dishes_db")).
        load()
      businessDF.registerTempTable("businesses")


      val dishQuery = "SELECT businessid, avgrating, numreviews, promotext FROM dishes WHERE citystate = '" + citystate +  "' AND dish IN (" + dishes + ")"
      val dishesFilteredDF: DataFrame = sqlContext.sql(dishQuery)

      val businessQuery = "SELECT businessid, full_address, name, reviewcount, stars FROM businesses WHERE citystate = '" + citystate + "'"
      val businessFilteredDF: DataFrame = sqlContext.sql(businessQuery)

      val joined = dishesFilteredDF.join(businessFilteredDF, dishesFilteredDF("businessid") === businessFilteredDF("businessid")).drop(businessFilteredDF("businessid"))

      val sorted = joined.rdd.sortBy(row => -row.getAs[Double]("avgrating"))

      val recommendations = sorted.take(5).map(row => {
        val id = row.getAs[String]("businessid")
        val avgrating = row.getAs[Double]("avgrating")
        val dishnumreviews = row.getAs[Int]("numreviews")
        val promotext = row.getAs[String]("promotext")
        val address = row.getAs[String]("full_address").replaceAll("\\\\n", " ")
        val name = row.getAs[String]("name")
        val restaurantnumreviews = row.getAs[Int]("reviewcount")
        val stars = row.getAs[Double]("stars")

        new DataPoint(id, avgrating, dishnumreviews, promotext, address, name, restaurantnumreviews, stars)
      }).toList

      recommendations

    }else{
      //search for specific dish

//      val reviewsRDD = dishesRDD.select("businessid","avgrating","numreviews","promoscore", "promotext", "totalscore").
//        where("citystate = ? AND dish = ?",citystate, dish).map(row => {
//
//          val id = row.getString("businessid")
//          val avgrating = row.getDouble("avgrating")
//          val numreviews = row.getInt("numreviews")
//          val promoscore = row.getInt("promoscore")
//          val promotext = row.getString("promotext")
//          val totalscore = row.getInt("totalscore")
//
//        Row(id, avgrating, numreviews, promoscore, promotext, totalscore)
//      })


      val dishesDF = sqlContext.
        read.
        format("org.apache.spark.sql.cassandra").
        options(Map("table" -> "dishes", "keyspace" -> "dishes_db")).
        load()
      dishesDF.registerTempTable("dishes")

      val businessDF = sqlContext.
        read.
        format("org.apache.spark.sql.cassandra").
        options(Map("table" -> "businesses", "keyspace" -> "dishes_db")).
        load()
      businessDF.registerTempTable("businesses")
      businessDF.cache()

      val dishesFilteredDF: DataFrame = sqlContext.sql("SELECT businessid, avgrating, numreviews, promotext FROM dishes WHERE citystate = '" + citystate +  "' AND dish = '" + dish + "'")
      val businessFilteredDF: DataFrame = sqlContext.sql("SELECT businessid, full_address, name, reviewcount, stars FROM businesses WHERE citystate = '" + citystate + "'")

      val joined = dishesFilteredDF.join(businessFilteredDF, dishesFilteredDF("businessid") === businessFilteredDF("businessid")).drop(businessFilteredDF("businessid"))

      val sorted = joined.rdd.sortBy(row => -row.getAs[Double]("avgrating"))

      val recommendations = sorted.take(5).map(row => {
        val id = row.getAs[String]("businessid")
        val avgrating = row.getAs[Double]("avgrating")
        val dishnumreviews = row.getAs[Int]("numreviews")
        val promotext = row.getAs[String]("promotext")
        val address = row.getAs[String]("full_address")
        val name = row.getAs[String]("name")
        val restaurantnumreviews = row.getAs[Int]("reviewcount")
        val stars = row.getAs[Double]("stars")

        new DataPoint(id, avgrating, dishnumreviews, promotext, address, name, restaurantnumreviews, stars)
      }).toList

      recommendations
//      val topreviews = reviewsRDD.sortBy(row => -row.getDouble("avgrating")).take(5).map(row => {
//
//        val id = row.getString("businessid")
//        val avgrating = row.getDouble("avgrating")
//        val numreviews = row.getInt("numreviews")
//        val promoscore = row.getInt("promoscore")
//        val promotext = row.getString("promotext")
//        val totalscore = row.getInt("totalscore")
//
//        new Review(id,avgrating,numreviews,promoscore,promotext,totalscore)
//
//      })

//      val reviewSchema = StructType(Array(StructField("businessid",StringType,true),StructField("avgrating",DoubleType,true),StructField("totalscore",IntegerType,true),StructField("numreviews",IntegerType,true),StructField("promotext",StringType,true),StructField("promoscore",IntegerType,true)))
//      val reviewsToStore = sqlContext.createDataFrame(reviewsRDD, reviewSchema)
//
//      val businessIds = topreviews.map(x => x.businessid).toList
//      val businesses = businessRDD.select("businessid","full_address","name","reviewcount", "stars").
//        where("citystate = ? AND businessid IN ?", citystate, businessIds).toDF
//      //      join business info with review on business id and return to client
//
//
//      topreviews
    }

    //println(citystate)
    //println(dish)

    //results(0).toMap
    //val dp = new DataPoint("survived", survived)
    //dp
    //results
  }

  override def render(value: JValue)(implicit formats: Formats): JValue = value.camelizeKeys


  def getCategories(fileName: String) = {

    val categoryItems = collection.mutable.Map[String, collection.mutable.Set[String]]()
    //val items = items: collection.mutable.Set[String]
    val source = scala.io.Source.fromFile(fileName);

    for (line <- source.getLines) {

      val tokens = line.split(":");
      val categoryLine = tokens(0);
      val item = tokens(1);

      //items.add(item)

      val categories = categoryLine.split('|');

      categories.foreach(category => {

        val itemSet = categoryItems.getOrElse(category, collection.mutable.Set[String]());
        itemSet.add(item);
        categoryItems.put(category, itemSet);
      })
    }
    categoryItems
  }
}
