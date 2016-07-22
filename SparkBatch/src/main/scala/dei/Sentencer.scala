package dei

import java.util.regex._
import scala.collection.JavaConversions._

object Sentencer extends Serializable {

  def getSentence(sentence: String, lemma: String): String = {

    var returnSentence = sentence

    if (sentence.length() > 300){

      val lemmaIndex: Int = sentence.indexOf(lemma)
      val startIndex: Int = if ((lemmaIndex - 150) < 0) 0 else lemmaIndex - 150
      val endIndex: Int = if ((startIndex + 300) > sentence.length() - 1) sentence.length() - 1 else startIndex + 300
      returnSentence = sentence.substring(startIndex, endIndex)
    }

    returnSentence
  }

  val re = Pattern.compile("[^.!?\\s][^.!?]*(?:[.!?](?!['\"]?\\s|$)[^.!?]*)*[.!?]?['\"]?(?=\\s|$)",
      Pattern.MULTILINE | Pattern.COMMENTS)

  def getSentences(text: String): List[String] = {

    val reMatcher = re.matcher(text)
    val sentences = new java.util.ArrayList[String]()

    while (reMatcher.find()) {

      //remove non-printable characters that might cause issues with coreNLP
      println("removing non-printable characters for : " + reMatcher.group())
      val printableCharactersSentence = reMatcher.group().replaceAll("\\p{C}", "");
      println(reMatcher.group())
      System.out.flush()

      sentences.add(printableCharactersSentence)
    }

    sentences.toList
  }
}