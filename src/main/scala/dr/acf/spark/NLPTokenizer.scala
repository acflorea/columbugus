package dr.acf.spark

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.{PartOfSpeechAnnotation, TextAnnotation, TokensAnnotation, SentencesAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{StringType, ArrayType, DataType}
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
  props.setProperty("tokenizerOptions", "untokenizable=noneDelete")
  val wrapper = new StanfordCoreNLPWrapper(props)

  override protected def createTransformFunc: String => Seq[String] = { text =>

    val pipeline = wrapper.get

    // create an empty Annotation just with the given text
    val document = new Annotation(text)

    // run all Annotators on this text
    pipeline.annotate(document)

    // these are all the sentences in this document
    val sentences = document.get(classOf[SentencesAnnotation]).asScala

    sentences flatMap { sentence =>
      // traversing the words in the current sentence
      // a CoreLabel is a CoreMap with additional token-specific methods
      sentence.get(classOf[TokensAnnotation]).asScala filter { token =>
        // this is the POS tag of the token - ONLY NOUNS
        token.get(classOf[PartOfSpeechAnnotation]).startsWith("NN")
      } map { token =>
        // this is the text of the token
        token.get(classOf[TextAnnotation])
      }
    }
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, true)

  override def copy(extra: ParamMap): NLPTokenizer = defaultCopy(extra)
}