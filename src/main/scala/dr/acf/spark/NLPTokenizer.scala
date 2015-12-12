package dr.acf.spark

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.{PartOfSpeechAnnotation, TextAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.Annotation
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Created by aflorea on 06.12.2015.
  */
class NLPTokenizer(override val uid: String)
  extends UnaryTransformer[String, Seq[String], NLPTokenizer] {

  def this() = this(Identifiable.randomUID("NLPTok"))

  // creates a StanfordCoreNLP object, with POS tagging
  val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, pos")
  val wrapper = new StanfordCoreNLPWrapper(props)

  val logger = LoggerFactory.getLogger(getClass.getName)

  override protected def createTransformFunc: String => Seq[String] = { text =>

    val currentTime = System.currentTimeMillis()

    val pipeline = wrapper.get

    // create an empty Annotation just with the given text
    val document = new Annotation(text)

    // run all Annotators on this text
    pipeline.annotate(document)

    val tokens = document.get(classOf[TokensAnnotation]).asScala collect {
      case token if token.get(classOf[PartOfSpeechAnnotation]).startsWith("NN") =>
        token.get(classOf[TextAnnotation])
    }

    logger.debug(s"${text.length} chars tokenized in ${System.currentTimeMillis() - currentTime}")

    tokens
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, true)

  override def copy(extra: ParamMap): NLPTokenizer = defaultCopy(extra)
}