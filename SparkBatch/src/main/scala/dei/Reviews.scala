package dei

import org.apache.spark.storage.StorageLevel

class Reviews (sqlContext: org.apache.spark.sql.SQLContext) extends Serializable{

  val reviewsDataSetFile = "s3://roy-data/yelp_reviews.parquet"
  val tipsDataSetFile = "s3://roy-data/yelp_tips.parquet"

  val reviews = sqlContext.read.parquet(reviewsDataSetFile)
  reviews.persist(StorageLevel.MEMORY_AND_DISK)

  val tips = sqlContext.read.parquet(tipsDataSetFile)
  tips.persist(StorageLevel.MEMORY_AND_DISK)

}
