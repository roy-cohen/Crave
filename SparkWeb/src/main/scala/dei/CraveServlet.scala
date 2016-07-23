package dei

import java.util

import com.datastax.driver.core.Row
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
import com.datastax.spark.connector.cql.CassandraConnector

import scala.collection.JavaConversions._
import scala.collection.mutable



class CraveServlet extends ScalatraServlet with JacksonJsonSupport {
  protected implicit val jsonFormats: Formats = DefaultFormats
  case class DataPoint(dish: String, businessid: String, avgrating: String, dishnumreviews: Integer, promotext:String, address:String, name:String, restaurantnumreviews:Int, stars:Double) extends java.io.Serializable
  case class BusinessObj(id: String, address: String, name: String, reviewcount: Int, stars: Double)
  case class City(citystate: String)

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

  val connector = CassandraConnector(sc.getConf)


  before() {
    contentType = formats("json")
  }

  get ("/") {

  }

  get("/dishes.json") {
    val file = System.getProperty("user.dir") + "/static/dishes.json"
    val dishes = scala.io.Source.fromFile(file).getLines.mkString
    dishes
  }

  get("/cities.json") {
    //val file = System.getProperty("user.dir") + "/static/cities.json"
    val cities = getCities()
    cities
  }

  get("/getTopDishesByCity.json") {

    val citystate = params.get("city").get.toString.replaceAll(",", "").replaceAll(" ", "")
    val recommendations = getTopDishesByCity(citystate)
    recommendations
  }

  get("/getTopDishesForRestaurant.json") {

    val citystate = params.get("city").get.toString.replaceAll(",", "").replaceAll(" ", "")
    val businessid = params.get("id").get.toString
    val recommendations = getTopDishesForRestaurant(citystate, businessid)
    recommendations
  }

  get("/searchDishes.json") {

    val citystate = params.get("city").get.toString.replaceAll(",","").replaceAll(" ","")
    val dish = params.get("dish").get.toString

    val dishes = categoryItems(dish).mkString("'", "','", "'")
    val recommendations = getDishes(citystate, dishes)

    recommendations
  }

  override def render(value: JValue)(implicit formats: Formats): JValue = value.camelizeKeys

  def getBusinessMap(rows: java.util.List[com.datastax.driver.core.Row] ) = {

    val businessMap = collection.mutable.Map[String, BusinessObj]()
    rows.map(row => {
      val id = row.getString("businessid")
      val address = row.getString("full_address").replaceAll("\\\\n", " ")
      val name = row.getString("name")
      val reviewcount = row.getInt("reviewcount")
      val stars = row.getDouble("stars")
      businessMap.put(id, new BusinessObj(id,address,name,reviewcount,stars))
    })

    businessMap
  }

  def roundAt(p: Int)(n: Double): Double = { val s = math pow (10, p); (math round n * s) / s }


  def getRecommendationObjects(top: mutable.Buffer[(Double, com.datastax.driver.core.Row)], businessMap: collection.mutable.Map[String,BusinessObj]) = {

    val recommendations = top.map{case(score,row) =>
      val dish = row.getString("dish")
      val id = row.getString("businessid")
      val tmp = row.getDouble("avgrating")
      val avgrating : String = f"$tmp%1.1f";
      val dishnumreviews = row.getInt("numreviews")
      val promotext = row.getString("promotext")
      val address = businessMap(id).address
      val name = businessMap(id).name
      val restaurantnumreviews = businessMap(id).reviewcount
      val stars = businessMap(id).stars

      new DataPoint(dish, id, avgrating, dishnumreviews, promotext, address, name, restaurantnumreviews, stars)
    }.toList

    recommendations
  }

  def getDishes(citystate: String, dishes: String) = {

      val session = connector.openSession

      val reviews = session.execute("SELECT dish, businessid, avgrating, numreviews, promotext FROM dishes_db.dishes WHERE citystate = ? AND dish IN (" + dishes +")", citystate)
      val top = getRecommendations(reviews.all).take(4)

      val businessids = top.map{case(score,row) => row.getString("businessid")}.mkString("'", "','", "'")
      val businesses = session.execute("SELECT businessid, full_address, name, reviewcount, stars FROM dishes_db.businesses WHERE citystate = ? AND businessid IN (" + businessids + ")", citystate)
      val bRows = businesses.all

      // Close session
      session.close

      val businessMap = getBusinessMap(bRows)
      val recommendations = getRecommendationObjects(top, businessMap)
      recommendations
  }

  def getCities() = {

    val session = connector.openSession

    val rows = session.execute("SELECT distinct citystate FROM dishes_db.dishes")
    val citystates = rows.all

    val cities = citystates.sortBy(row => row.getString("citystate")).map(row => {
      val citystate = row.getString("citystate")
      val index = citystate.length() - 2
      val city = citystate.substring(0, index)
      val state = citystate.substring(index)

      city + ", " + state
    }).toList
    // Close session
    session.close

    cities
  }

  def getTopDishesByCity(citystate:String) = {

    val session = connector.openSession

    val reviews = session.execute("SELECT dish, businessid, avgrating, numreviews, promotext FROM dishes_db.dishes WHERE citystate = ?", citystate)
    val top = getRecommendations(reviews.all).take(3)

    val businessids = top.map{case(score,row) => row.getString("businessid")}.mkString("'","','","'")
    val businesses = session.execute("SELECT businessid, full_address, name, reviewcount, stars FROM dishes_db.businesses WHERE citystate = ? AND businessid IN (" + businessids + ")", citystate)
    val bRows = businesses.all

    // Close session
    session.close

    val businessMap = getBusinessMap(bRows)
    val recommendations = getRecommendationObjects(top, businessMap)
    recommendations
  }

  def getTopDishesForRestaurant(citystate:String, businessid: String) = {

    val session = connector.openSession

    val reviews = session.execute("SELECT dish, businessid, avgrating, numreviews, promotext FROM dishes_db.dishes WHERE citystate = ? AND businessid = ?", citystate, businessid)
    val top = getRecommendations(reviews.all).take(3)

    val businesses = session.execute("SELECT businessid, full_address, name, reviewcount, stars FROM dishes_db.businesses WHERE citystate = ? AND businessid = ?", citystate, businessid)
    val bRows = businesses.all

    // Close session
    session.close

    val businessMap = getBusinessMap(bRows)
    val recommendations = getRecommendationObjects(top, businessMap)
    recommendations
  }

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

  def getRecommendations(rows: util.List[com.datastax.driver.core.Row]) : mutable.Buffer[(Double, com.datastax.driver.core.Row)] = {

    val filteredScores = rows.filter(row => row.getDouble("avgrating") >= 3.5)

    val weightedScores = filteredScores.map(row => {

      val numreviews = row.getInt("numreviews")
      val avgrating = row.getDouble("avgrating")

      val weight = numreviews match {
        case x if (x >= 0 && x < 10) => 0.55;
        case x if (x >= 10 && x < 25) => 0.60;
        case x if (x >= 25 && x < 50) => 0.65;
        case x if (x >= 50 && x < 75) => 0.70;
        case x if (x >= 75 && x < 100) => 0.75;
        case x if (x >= 100 && x < 125) => 0.80;
        case x if (x >= 125 && x < 150) => 0.85;
        case x if (x >= 150 && x < 175) => 0.90;
        case x if (x >= 175 && x < 500) => 0.95;
        case x if (x >= 500) => 1.00;
      }

      val weightedScore = avgrating * weight

      (weightedScore, row)
    })

    val recommendations = weightedScores.sortBy{case (score,row) => -score}
    recommendations
  }
}
