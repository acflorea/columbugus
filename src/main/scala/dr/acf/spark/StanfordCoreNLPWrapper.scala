package dr.acf.spark

import java.util.Properties

import edu.stanford.nlp.pipeline.StanfordCoreNLP

/**
  * Created by aflorea on 06.12.2015.
  */
class StanfordCoreNLPWrapper(private val props: Properties) extends Serializable {
  @transient private var coreNLP: StanfordCoreNLP = _

  /** Returns the contained [[StanfordCoreNLP]] instance. */
  def get: StanfordCoreNLP = {
    if (coreNLP == null) {
      coreNLP = new StanfordCoreNLP(props)
    }
    coreNLP
  }
}
