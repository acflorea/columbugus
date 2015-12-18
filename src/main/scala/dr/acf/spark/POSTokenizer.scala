package dr.acf.spark

import edu.stanford.nlp.ling.CoreAnnotations.{PartOfSpeechAnnotation, TextAnnotation, TokensAnnotation}
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}
import org.slf4j.LoggerFactory

/**
  * Created by aflorea on 06.12.2015.
  */
class POSTokenizer(override val uid: String)
  extends UnaryTransformer[String, Seq[String], POSTokenizer] {

  def this() = this(Identifiable.randomUID("POSTok"))

  // Initialize the tagger
  val wrapper = new StanfordMaxentTaggerWrapper

  val logger = LoggerFactory.getLogger(getClass.getName)

  override protected def createTransformFunc: String => Seq[String] = { text =>

    val currentTime = System.currentTimeMillis()

    val tagger = wrapper.get

    val tokens = tagger.tagString(text).split(" ") collect {
      case wordAndTag if wordAndTag.split("_").last.startsWith("NN") =>
        wordAndTag.split("_").head
    }

    if (logger.isTraceEnabled()) {
      logger.trace(s"${text.length} chars tokenized in ${System.currentTimeMillis() - currentTime}")
    }

    tokens
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, true)

  override def copy(extra: ParamMap): POSTokenizer = defaultCopy(extra)
}