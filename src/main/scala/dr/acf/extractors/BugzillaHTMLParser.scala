package dr.acf.extractors

import java.io.{File, FilenameFilter}
import java.sql.Timestamp

import dr.acf.connectors.SlickConnector
import org.htmlcleaner.{CleanerProperties, HtmlCleaner, TagNode}
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

    val ROOT_FOLDER = "/mnt/Storage/#DATASOURCES/Bug_Recommender/4"

    val folder = new File(ROOT_FOLDER)

    val noHistory = new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = !name.contains("history")
    }

    val ids = (folder.listFiles(noHistory) map { f =>
      f.getName.split(".html").head
    }).filter(id => id.startsWith("51"))

    logger.debug(s"Processing ${ids.length} files")

    val assignmentsMap = new scala.collection.mutable.HashMap[String, Int]()
    val componentsMap = new scala.collection.mutable.HashMap[String, Int]()
    val productsMap = new scala.collection.mutable.HashMap[String, Int]()
    val fieldsMap = new scala.collection.mutable.HashMap[String, Int]()

    Await.result(db.run(DBIO.seq(
      assignments.result.map(_.foreach { case (id, name) => assignmentsMap.put(name, id) }),
      components.result.map(_.foreach { case (id, product_id, name) => componentsMap.put(name, id) }),
      products.result.map(_.foreach { case (id, classification_id, name) => productsMap.put(name, id) }),
      fields.result.map(_.foreach { case (id, name) => fieldsMap.put(name, id) })
    )), Duration.Inf)

    (ids zipWithIndex) foreach { id_index =>

      val id = id_index._1

      logger.debug(s"${ids.length - id_index._2} files remaining")

      val bugDataFile = new File(s"$ROOT_FOLDER/$id.html")
      val bugHistoryFile = new File(s"$ROOT_FOLDER/$id-history.html")

      if (!bugHistoryFile.exists()) {
        logger.debug(s"Skip bug $id. Missing history file")
      } else {

        val props = new CleanerProperties()
        props.setRecognizeUnicodeChars(true)
        val cleaner = new HtmlCleaner(props)

        val rootNode = cleaner.clean(bugDataFile, "UTF-8")
        val rootNodeHistory = cleaner.clean(bugHistoryFile, "UTF-8")

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
              else {
                val lastFullEntry = completeHistoryMap.filter(entry => entry._1 < historyEntry._1 && entry._2.length == 5).maxBy(_._1)
                historyEntry._1 -> (lastFullEntry._2.take(2) ++ historyEntry._2)
              }
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
            val longdescsWho = changeForm.evaluateXPath("/table/tbody/tr/td/div[@id='comments']/div/div/span[@class='bz_comment_user']").
              map(_.asInstanceOf[TagNode].getText.toString.trim)


            // STORE !!!

            logger.debug(s"BUG ID :: $bug_id")

            try {

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

              completeHistory map { historyEntry =>
                val field_id = fieldsMap.get(historyEntry(2)) match {
                  case Some(_id) => _id
                  case None => fieldsMap.put(historyEntry(2), fieldsMap.size + 1)
                    Await.result(db.run(fields +=(fieldsMap.size, historyEntry(2))), Duration.Inf)
                    fieldsMap.size
                }
                val who = assignmentsMap.get(historyEntry(0)) match {
                  case Some(_id) => _id
                  case None => assignmentsMap.put(historyEntry(0), assignmentsMap.size + 1)
                    Await.result(db.run(assignments +=(assignmentsMap.size, historyEntry(0))), Duration.Inf)
                    assignmentsMap.size
                }

                Await.result(db.run(bug_activities +=(bug_id, who, field_id, toPDTDate(historyEntry(1)), historyEntry(3), historyEntry(4))), Duration.Inf)
              }

              (longdescsHead zip longdescsBody zip longdescsWho) map {
                longdesc =>

                  val who = assignmentsMap.get(longdesc._2) match {
                    case Some(_id) => _id
                    case None => assignmentsMap.put(longdesc._2, assignmentsMap.size + 1)
                      Await.result(db.run(assignments +=(assignmentsMap.size, longdesc._2)), Duration.Inf)
                      assignmentsMap.size
                  }
                  Await.result(db.run(longdescs +=(bug_id, who, longdesc._1._1, longdesc._1._2)), Duration.Inf)
              }

              duplicateOf map {
                dupe_of => Await.result(db.run(duplicates +=(bug_id, Integer.valueOf(dupe_of))), Duration.Inf)
              }

            } catch {
              case e: Exception => logger.error("Encoding issue ? ", e)
            }

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
