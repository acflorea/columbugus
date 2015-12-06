package dr.acf.experiments

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations._
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}

import scala.collection.JavaConverters._

/**
  * Created by aflorea on 05.12.2015.
  */
object NLPTest {
  def main(args: Array[String]) {

    // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos")
    val pipeline = new StanfordCoreNLP(props)

    // read some text in the text variable
    val text = "The quick brown fox jumps over the lazy dog"

    // create an empty Annotation just with the given text
    val document = new Annotation(text)

    // run all Annotators on this text
    pipeline.annotate(document)

    // these are all the sentences in this document
    // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
    val sentences = document.get(classOf[SentencesAnnotation]).asScala

    sentences foreach { sentence =>
      // traversing the words in the current sentence
      // a CoreLabel is a CoreMap with additional token-specific methods
      val tokens = sentence.get(classOf[TokensAnnotation]).asScala
      tokens foreach { token =>
        // this is the text of the token
        val word = token.get(classOf[TextAnnotation])
        // this is the POS tag of the token
        val pos = token.get(classOf[PartOfSpeechAnnotation])
        // this is the NER label of the token
        // val ne = token.get(classOf[NamedEntityTagAnnotation])

        println(s"Word: $word, POS: $pos")
      }
    }

  }
}
