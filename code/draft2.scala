import java.util._
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling._
import edu.stanford.nlp.ling.CoreAnnotations._
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.simple._;
import scala.collection.JavaConversions._

val sample = sqlContext.read.json("input/yelp_academic_dataset_review.json")
//val reviews = sqlContext.read.json("input/yelp_academic_dataset_review.json")
val reviews = sample.sample(false,0.0001)
//reviews.show


val scores = reviews.mapPartitions( rows => {
  // Create core NLP
  val props = new Properties()
  props.put("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
  val coreNLP = new StanfordCoreNLP(props, false)

  rows.map{ row => {

      val review = row.getAs[String]("text")
  val doc = new Document(review);
  var item = "";
  var score = -1;

  for (sentence <- doc.sentences()) {  
    
    for (lemma <- sentence.lemmas()) {  
            
           if (items.contains(lemma)){
              item = lemma;
           }
        }

        if (item != ""){
          //score = getSentiment(sentence.toString)
          val scoreArray = coreNLP.process(sentence.toString).
          get(classOf[CoreAnnotations.SentencesAnnotation]).
          map(_.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])).
          map(RNNCoreAnnotations.getPredictedClass(_))
          score = scoreArray(0) + 1
      }
    }

    (row.getAs[String]("business_id"), item, score, review)

    }}
  })