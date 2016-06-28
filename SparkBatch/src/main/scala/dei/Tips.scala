package dei

/**
  * Created by roycohen on 6/26/16.
  */
class Tips extends Reviews{

  val tipSample = App.sqlContext.read.json("input/yelptipmin.json") //in local mode only
  val tips = tipSample.sample(false,1)
  tips.cache
}
