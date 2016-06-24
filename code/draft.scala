def getLemmas(text:String) = {
  coreNLP.process(text).
    get(classOf[TokensAnnotation]).
    map(_.get(classOf[LemmaAnnotation]))
}

def getSentences(text:String) = {
  coreNLP.process(text).get(classOf[CoreAnnotations.SentencesAnnotation])  
}

def getSentiment(text:String) = {
  coreNLP.process(text).
    get(classOf[CoreAnnotations.SentencesAnnotation]).
    map(_.get(
      classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])).
    map(RNNCoreAnnotations.getPredictedClass(_))
}


reviews.filter(reviews("text") contains "burger").show
1. break into sentences with core nlp
2. lowercase
3. loop through items and check if sentence contains one of them (return item) otherwise return empty string
4. filter on sentences with item which is not empty
5. do a sentiment analysis on the sentence
6. at this point I should have the businessId, the item (these are the primary keys in cassandra), the sentiment score, and the sentence (could you use for promo)



val s = new Sentence("hamburgers")
s.lemma(0)



import scala.collection.convert.wrapAll._
val props = new java.util.Properties()
props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
val pipeline = new StanfordCoreNLP(props)
val annotation: Annotation = pipeline.process(text)



object SentimentAnalyzer {

  val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

  def mainSentiment(input: String): Int = Option(input) match {
    case Some(text) if !text.isEmpty => extractSentiment(text)
    case _ => throw new IllegalArgumentException("input can't be null or empty")
  }

  private def extractSentiment(text: String): Int = {
    val (_, sentiment) = extractSentiments(text)
      .maxBy { case (sentence, _) => sentence.length }
    sentiment
  }

  def extractSentiments(text: String): List[(String, Int)] = {
    val annotation: Annotation = pipeline.process(text)
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    sentences
      .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
      .map { case (sentence, tree) => (sentence.toString,RNNCoreAnnotations.getPredictedClass(tree)) }
      .toList
  }
}

val input = "this hamburger sucks!"
val sentiment = SentimentAnalyzer.mainSentiment(input)


//https://github.com/shekhargulati/52-technologies-in-2016/blob/master/03-stanford-corenlp/README.md
