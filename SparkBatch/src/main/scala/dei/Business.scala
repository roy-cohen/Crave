package dei

import org.apache.spark.storage.StorageLevel

class Business (sqlContext: org.apache.spark.sql.SQLContext){

  val businessDataSetFile = "s3://roy-data/yelp_business.json"
  val businesses = sqlContext.read.json(businessDataSetFile)
  businesses.persist(StorageLevel.MEMORY_AND_DISK)
}
