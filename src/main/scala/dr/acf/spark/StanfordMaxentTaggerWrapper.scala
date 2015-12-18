package dr.acf.spark

import java.util.Properties

import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.tagger.maxent.MaxentTagger

/**
  * Created by aflorea on 06.12.2015.
  */
class StanfordMaxentTaggerWrapper extends Serializable {
  @transient private var maxentTagger: MaxentTagger = _

  /** Returns the contained [[MaxentTagger]] instance. */
  def get: MaxentTagger = {
    if (maxentTagger == null) {
      maxentTagger = new MaxentTagger("taggers/english-left3words-distsim.tagger")
    }
    maxentTagger
  }
}
