package dr.acf.extractors

import java.io.{File, FilenameFilter}
import java.sql.Timestamp

import dr.acf.connectors.SlickConnector
import org.htmlcleaner.{HtmlCleaner, TagNode}
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory
import slick.driver.MySQLDriver.api._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

/**
  * Created by aflorea on 15.11.2015.
  */
object BugzillaHTMLParser extends SlickConnector {

  val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]) {

    val ROOT_FOLDER = "/mnt/Storage/#DATASOURCES/Bug_Recommender/test"

    val folder = new File(ROOT_FOLDER)

    val noHistory = new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = !name.contains("history")
    }

    val ids = folder.listFiles(noHistory) map { f =>
      f.getName.split(".html").head
    }

    val assignmentsMap = new scala.collection.mutable.HashMap[String, Int]()
    val componentsMap = new scala.collection.mutable.HashMap[String, Int]()
    val productsMap = new scala.collection.mutable.HashMap[String, Int]()

    Await.result(db.run(DBIO.seq(
      assignments.result.map(_.foreach { case (id, name) => assignmentsMap.put(name, id) }),
      components.result.map(_.foreach { case (id, product_id, name) => componentsMap.put(name, id) }),
      products.result.map(_.foreach { case (id, classification_id, name) => productsMap.put(name, id) })
    )), Duration.Inf)

    ids foreach { id =>

      val bugDataFile = new File(s"$ROOT_FOLDER/$id.html")
      val bugHistoryFile = new File(s"$ROOT_FOLDER/$id-history.html")

      if (!bugHistoryFile.exists()) {
        logger.debug(s"Skip bug $id. Missing history file")
      } else {

        val cleaner = new HtmlCleaner()
        val props = cleaner.getProperties
        val rootNode = cleaner.clean(bugDataFile)
        val rootNodeHistory = cleaner.clean(bugHistoryFile)

        val historyTable = rootNodeHistory.
          evaluateXPath("/body/div[@id='bugzilla-body']/table/tbody/tr")

        val historyEntries = historyTable collect {
          case tr: TagNode if tr.evaluateXPath("/td").nonEmpty =>
            tr.evaluateXPath("/td").collect {
              case td: TagNode => td.getText.toString.replace("\n", "").trim
            }
        }

        // Bug information columns
        val changeForm = rootNode.findElementByAttValue("name", "changeform", true, true)
        if (changeForm == null) {
          logger.debug(s"Login required for $id. Skipping")
        } else {
          val bz_show_bug_column_1 = changeForm.findElementByAttValue("id", "bz_show_bug_column_1", true, true)
          val bz_show_bug_column_2 = changeForm.findElementByAttValue("id", "bz_show_bug_column_2", true, true)

          //    bug_id
          val bug_id = Integer.valueOf(changeForm.findElementByAttValue("name", "id", true, true).getAttributeByName("value"))

          //    assigned_to
          val doubleSpan = bz_show_bug_column_1.evaluateXPath("/table/tbody/tr[12]/td[2]/span/span")
          val assigned_to_str = (if (doubleSpan.nonEmpty) {
            doubleSpan(0).asInstanceOf[TagNode].getText
          } else {
            bz_show_bug_column_1.evaluateXPath("/table/tbody/tr[12]/td[2]/span")(0).asInstanceOf[TagNode].getText
          }).toString.replace("\n", "").trim

          //    bug_severity
          val bug_severity = bz_show_bug_column_1.evaluateXPath("/table/tbody/tr[10]/td[2]")(0).asInstanceOf[TagNode].
            getText.toString.split("\n")(1).trim

          //    bug_status
          val bug_status = changeForm.findElementByAttValue("id", "static_bug_status", true, true).
            getText.toString.split("\n").head.trim

          //    creation_ts
          val creation_ts =
            toPDTDate(bz_show_bug_column_2.evaluateXPath("/table/tbody/tr[1]/td[2]")(0).
              asInstanceOf[TagNode].getText.toString.substring(0, 20), "yyyy-MM-dd HH:mm")

          if (historyEntries.isEmpty) {
            logger.debug(s"History is empty for $id. Skipping")
          }
          else {

            val completeHistoryMap = (historyEntries zipWithIndex) map (_.swap) toMap

            val completeHistory = (completeHistoryMap map {
              historyEntry => if (historyEntry._2.length == 5)
                historyEntry
              else
                historyEntry._1 -> (completeHistoryMap.get(historyEntry._1 - 1).get.take(2) ++ historyEntry._2)
            }).toSeq.sortBy(_._1).map(_._2)

            // delta_ts
            val delta_ts = toPDTDate(historyEntries.filter(_.length == 5).
              last(1).split("\n").head)

            //    short_desc
            val short_desc = changeForm.findElementByAttValue("id", "short_desc_nonedit_display", true, true).
              getText.toString

            //    resolution
            val resolution = changeForm.findElementByAttValue("id", "static_bug_status", true, true).
              getText.toString.split("\n").drop(1).head.trim

            // duplicate of
            val duplicateOf = if (resolution == "DUPLICATE") {
              Some(changeForm.findElementByAttValue("id", "static_bug_status", true, true).
                getText.toString.split("\n").drop(2).head.trim.split(" ").last)
            } else {
              None
            }

            //    product_id
            val product_id_str = bz_show_bug_column_1.evaluateXPath("/table/tbody/tr[5]/td[1]")(0).asInstanceOf[TagNode].
              getText.toString

            //    component_id
            val component_id_str = bz_show_bug_column_1.evaluateXPath("/table/tbody/tr[6]/td[2]")(0).asInstanceOf[TagNode].
              getText.toString.replace("\n", "").trim


            // Long descs
            val longdescsHead =
              (changeForm.evaluateXPath("/table/tbody/tr/td/div[@id='comments']/div/div[@class='bz_first_comment_head']/span[@class='bz_comment_time']") ++
                changeForm.evaluateXPath("/table/tbody/tr/td/div[@id='comments']/div/div[@class='bz_comment_head']/span[@class='bz_comment_time']")).
                map(node => toPDTDate(node.asInstanceOf[TagNode].getText.toString.replace("\n", "").trim))
            val longdescsBody = changeForm.evaluateXPath("/table/tbody/tr/td/div[@id='comments']/div/pre[@class='bz_comment_text']").
              map(_.asInstanceOf[TagNode].getText.toString.trim)


            // STORE !!!
            val product_id = productsMap.get(product_id_str) match {
              case Some(_id) => _id
              case None => productsMap.put(product_id_str, productsMap.size + 1)
                Await.result(db.run(products +=(productsMap.size, -1, product_id_str)), Duration.Inf)
                productsMap.size
            }

            val component_id = componentsMap.get(component_id_str) match {
              case Some(_id) => _id
              case None => componentsMap.put(component_id_str, componentsMap.size + 1)
                Await.result(db.run(components +=(componentsMap.size, product_id, component_id_str)), Duration.Inf)
                componentsMap.size
            }

            val assign_to = assignmentsMap.get(assigned_to_str) match {
              case Some(_id) => _id
              case None => assignmentsMap.put(assigned_to_str, assignmentsMap.size + 1)
                Await.result(db.run(assignments +=(assignmentsMap.size, assigned_to_str)), Duration.Inf)
                assignmentsMap.size
            }

            Await.result(db.run(bugs +=(bug_id, creation_ts, short_desc, bug_status, assign_to, component_id, bug_severity, resolution, delta_ts)), Duration.Inf)

            val bugData = BugData(-1, Integer.valueOf(bug_id), short_desc, resolution, -1, -1, -1, "<>", null)

            println(bugData, assigned_to_str, component_id_str, bug_status)

            completeHistory map (_.mkString(",")) foreach println
          }
        }
      }
    }
  }


  def toPDTDate(fullDate: String, pattern: String = "yyyy-MM-dd HH:mm:ss"): Timestamp = {
    val formatter = DateTimeFormat.forPattern(pattern)
    val timeZone = fullDate.substring(fullDate.length - 3)
    val actualDate = fullDate.substring(0, fullDate.length - 4)
    val dt = formatter.withZone(DateTimeZone.forID("PST8PDT")).parseDateTime(actualDate)
    new Timestamp(dt.getMillis)
  }

}