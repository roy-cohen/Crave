package dei

import java.util.Properties
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.simple.Document
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import scala.collection.JavaConversions._

object Processor {

  def getScores (reviews: org.apache.spark.sql.DataFrame, items: scala.collection.mutable.Set[String]) : RDD[(String, ReviewInfo)] = {

    val scores = reviews.repartition(76).mapPartitions( rows => {

      val props = new Properties()
      props.put("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")

      val wrapper = new StanfordCoreNLPWrapper(props)
      val coreNLP = wrapper.get
      var doc = new Document("");

      rows.map ( row => {

        val review = row.getAs[String]("text")
        val date = row.getAs[String]("date")
        var sentenceWithItem = ""
        var item = "";
        var score = -1;

        var sentenceList = Sentencer.getSentences(review)

        for (sentence <- sentenceList) {

          doc = new Document(sentence);

          if (doc.sentences.length > 0) {
            val lemmas = doc.sentence(0).lemmas

            for (lemma <- lemmas) {

              if (items.contains(lemma)) {

                sentenceWithItem = Sentencer.getSentence(sentence, lemma)
                item = lemma;

                val scoreArray = coreNLP.process(sentenceWithItem).
                  get(classOf[CoreAnnotations.SentencesAnnotation]).
                  map(_.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])).
                  map(RNNCoreAnnotations.getPredictedClass(_))

                score = scoreArray(0) + 1

              }//if (items.contains(lemma)) {
            }//for (lemma <- lemmas) {
          }//if (doc.sentences.length > 0) {
        }//for (sentence <- sentenceList) {

        val key = row.getAs[String]("business_id") + "SEPARATOR" + item
        val reviewInfo = new ReviewInfo(score, sentenceWithItem, score, date)
        (key, reviewInfo)
      })
    })

    val filteredScores = scores.filter{ case(key, reviewInfo) => reviewInfo.score != -1}
    filteredScores
  }

  case class ReviewInfo(score:Int, promoText:String, promoScore:Int, promoDate:String) extends Product with Serializable
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


  def getAverageScores(filteredScores: RDD[(String, ReviewInfo)]) : RDD[Row] = {

    val groupedScores = filteredScores.combineByKey(createScoreCombiner, scoreCombiner, scoreMerger)

    val averageScores = groupedScores.map(averagingFunction).
      map{ case(key, avgscore, totalScore, numReviews, promoText, promoDate, promoScore) =>
        val tokens = key.split("SEPARATOR"); (tokens(0), tokens(1), avgscore, totalScore, numReviews, promoText, promoScore)
      }

    val returnRDD = averageScores.map(p => Row(p._1, p._2, p._3, p._4, p._5, p._6, p._7) )
    returnRDD
  }
}
