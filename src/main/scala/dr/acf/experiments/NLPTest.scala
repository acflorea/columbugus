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
    val text = "Another ex-Golden Stater, Paul Stankowski from Oxnard, is contending\n" +
      "for a berth on the U.S. Ryder Cup team after winning his first PGA Tour\n" +
      "event last year and staying within three strokes of the lead through\n" +
      "three rounds of last month's U.S. Open. H.J. Heinz Company said it\n" +
      "completed the sale of its Ore-Ida frozen-food business catering to the\n" +
      "service industry to McCain Foods Ltd. for about $500 million.\n" +
      "It's the first group action of its kind in Britain and one of\n" +
      "only a handful of lawsuits against tobacco companies outside the\n" +
      "U.S. A Paris lawyer last year sued France's Seita SA on behalf of\n" +
      "two cancer-stricken smokers. Japan Tobacco Inc. faces a suit from\n" +
      "five smokers who accuse the government-owned company of hooking\n" +
      "them on an addictive product."

    // create an empty Annotation just with the given text
    val document = new Annotation(text)

    // run all Annotators on this text
    (1 to 10000) foreach { i =>
      println(i)
      pipeline.annotate(document)
    }

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
