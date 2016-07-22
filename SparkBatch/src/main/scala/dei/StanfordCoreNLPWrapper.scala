package dei

import java.util.Properties
import edu.stanford.nlp.pipeline.StanfordCoreNLP

/* This class is taken from a gist by thomaspouncy
   https://gist.github.com/thomaspouncy/e620d32d636f764df261 */


/* This class provides a wrapper for the CoreNLP process object. The wrapper
 * extends Serializable so that Spark can serialize it and pass it to cluster nodes
 * for parallel processing, and it will create a CoreNLP instance lazily
 * Thanks to https://github.com/databricks/spark-corenlp for this idea */

class StanfordCoreNLPWrapper(private val props: Properties) extends Serializable {

  /* The transient annotation here tells Scala not to attempt to serialize the value
   * of the coreNLP variable. CoreNLP starts up a parallel JVM process and returns
   * information for connecting to that process, so even if the object were serializable
   * it wouldn't make any sense to pass those values since the process would not be
   * be guaranteed to be running on the other nodes. */
  @transient private var coreNLP: StanfordCoreNLP = _

  def get: StanfordCoreNLP = {
    if (coreNLP == null) {
      coreNLP = new StanfordCoreNLP(props)
    }
    coreNLP
  }
}