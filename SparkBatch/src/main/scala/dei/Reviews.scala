package dei

import org.apache.spark.SparkContext

/**
  * Created by roycohen on 6/25/16.
  */
class Reviews {

  import com.datastax.spark.connector._
  import org.apache.spark.SparkConf
  import java.util._
  import edu.stanford.nlp.pipeline._
  import edu.stanford.nlp.ling._
  import edu.stanford.nlp.ling.CoreAnnotations._
  import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
  import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
  import edu.stanford.nlp.simple.Document;
  import scala.collection.JavaConversions._
  import scala.util.control.Breaks._
  import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType,DoubleType};
  import org.apache.spark.sql.Row;


  val sample = App.sqlContext.read.json("input/yelpreviewmin.json") //in local mode only
  val reviews = sample.sample(false,0.1)
  reviews.cache


  case class ReviewInfo(score:Int, promoText:String, promoScore:Int, promoDate:String) extends Product with Serializable
  //case class ReviewInfo(score:Int, promoText:String, promoDate:String, promoScore:Int) extends Product with Serializable
  type ReviewCollector = (Int, ReviewInfo)
  type DishScores = (String, (Int, ReviewInfo))

  val createScoreCombiner = (review: ReviewInfo) => (1, review)

  val scoreCombiner = (collector: ReviewCollector, review1: ReviewInfo) => {
    var promoScore = 0;
    var promoText = "";
    var promoDate = "";
    val (numReviews, (review2)) = collector
    if (review1.promoScore > review2.promoScore) { promoText = review1.promoText; promoScore = review1.promoScore; promoDate = review1.promoDate; }
    else { promoText = review2.promoText; promoScore = review2.promoScore; promoDate = review2.promoDate; }

    val reviewInfoResult = new ReviewInfo(review1.score + review2.score, promoText, promoScore, promoDate)
    (numReviews + 1, reviewInfoResult)
  }

  val scoreMerger = (collector1: ReviewCollector, collector2: ReviewCollector) => {

    val (numReviews1, (review1)) = collector1
    val (numReviews2, (review2)) = collector2

    var promoScore = 0;
    var promoText = "";
    var promoDate = "";

    if (review1.promoScore > review2.promoScore) { promoText = review1.promoText; promoScore = review1.promoScore; promoDate = review1.promoDate; }
    else { promoText = review2.promoText; promoScore = review2.promoScore; promoDate = review2.promoDate; }

    val reviewInfoResult = new ReviewInfo(review1.score + review2.score, promoText, promoScore, promoDate)
    (numReviews1 + numReviews2, reviewInfoResult)
  }

  val averagingFunction = (dishScore: DishScores) => {
    val (key, (numReviews, reviewInfo)) = dishScore
    (key, (reviewInfo.score * 1.0 / numReviews), reviewInfo.score, numReviews, reviewInfo.promoText, reviewInfo.promoDate, reviewInfo.promoScore)
  }

  def getScores (reviews: org.apache.spark.sql.DataFrame, items: scala.collection.mutable.Set[String]) = {
    val scores = reviews.mapPartitions( rows => {
      // Create core NLP
      val props = new Properties()
      //props.put("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
      props.put("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
      val coreNLP = new StanfordCoreNLP(props, false)

      rows.map{ row => {

        val review = row.getAs[String]("text")
        val date = row.getAs[String]("date")
        val doc = new Document(review);
        var item = "";
        var score = -1;
        var sentenceWithItem = ""

        breakable { for (sentence <- doc.sentences()) {

          breakable { for (lemma <- sentence.lemmas()) {

            if (items.contains(lemma)){
              item = lemma;
              sentenceWithItem = sentence.text
              val scoreArray = coreNLP.process(sentenceWithItem).
                get(classOf[CoreAnnotations.SentencesAnnotation]).
                map(_.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])).
                map(RNNCoreAnnotations.getPredictedClass(_))
              score = scoreArray(0) + 1
              //break don't break so we get last item in sentence (optimally we should process all of the terms)
            }
          }}

          if (score != -1) break
        }}

        val key = row.getAs[String]("business_id") + "SEPARATOR" + item
        val reviewInfo = new ReviewInfo(score, sentenceWithItem, score, date)
        (key, reviewInfo)
      }}
    })

    val filteredScores = scores.filter{ case(key, reviewInfo) => reviewInfo.score != -1}
    val groupedScores = filteredScores.combineByKey(createScoreCombiner, scoreCombiner, scoreMerger)

    val averageScores = groupedScores.map(averagingFunction).
      map{ case(key, avgscore, totalScore, numReviews, promoText, promoDate, promoScore) =>
        val tokens = key.split("SEPARATOR"); (tokens(0), tokens(1), avgscore, totalScore, numReviews, promoText, promoScore)
      }

    //val step1 = scores.aggregateByKey((0.0, 0))((a, b) => (a._1 + b, a._2 + 1), (a, b) => (a._1 + b._1, a._2 + b._2))
    //val avgByKey = step1.mapValues(i => (i._1, i._2, i._1 * 1.0/i._2))
    //val dataToStore = avgByKey.map{ case(key, (total, num, myavg)) => val tokens = key.split("SEPARATOR"); (tokens(0), tokens(1), total, num, myavg, "", 0)}
    averageScores
  }
}
