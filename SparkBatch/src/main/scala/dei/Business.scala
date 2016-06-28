package dei

/**
  * Created by roycohen on 6/25/16.
  */
class Business {

  val sample2 = App.sqlContext.read.json("input/yelpbusinessmin.json") //in local mode only
  //val sample2a = sample2.sample(false,0.1)
  sample2.registerTempTable("businesses")
  val businesses = App.sqlContext.sql("SELECT business_id, name, full_address, city, state, review_count, stars FROM businesses")
  //.where(array_contains($"categories", "Restaurants"))
  businesses.cache
}
