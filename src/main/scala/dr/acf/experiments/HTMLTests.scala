package dr.acf.experiments

import java.io.{File, FilenameFilter}

import dr.acf.extractors.BugData
import org.htmlcleaner.{HtmlCleaner, TagNode}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.LoggerFactory

/**
  * Created by aflorea on 15.11.2015.
  */
object HTMLTests {

  val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]) {

    val ROOT_FOLDER = "/mnt/Storage/#DATASOURCES/Bug_Recommender/5"

    val folder = new File(ROOT_FOLDER)

    val noHistory = new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = !name.contains("history")
    }

    val ids = folder.listFiles(noHistory) map { f =>
      f.getName.split(".html").head
    }

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
              case td: TagNode => td.getText
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
          val bug_id = changeForm.findElementByAttValue("name", "id", true, true).getAttributeByName("value")

          //    assigned_to
          val doubleSpan = bz_show_bug_column_1.evaluateXPath("/table/tbody/tr[12]/td[2]/span/span")
          val assigned_to = if (doubleSpan.nonEmpty) {
            doubleSpan(0).asInstanceOf[TagNode].getText
          } else {
            bz_show_bug_column_1.evaluateXPath("/table/tbody/tr[12]/td[2]/span")(0).asInstanceOf[TagNode].getText
          }


          //    bug_file_loc
          //    bug_severity
          //    bug_status
          //    creation_ts
          //    delta_ts
          if (historyEntries.isEmpty) {
            logger.debug(s"History is empty for $id. Skipping")
          }
          else {
            val delta_ts: DateTime = toPDTDate(historyEntries.filter(_.length == 5).last(1).toString.split("\n").head)

            //    short_desc
            //    op_sys
            //    priority
            //    rep_platform
            //    reporter
            //    version
            //    resolution
            val resolution = changeForm.findElementByAttValue("id", "static_bug_status", true, true).
              getText.toString.split("\n").head

            //    target_milestone
            //    qa_contact
            //    status_whiteboard
            //    votes
            //    keywords
            //    lastdiffed
            //    everconfirmed
            //    reporter_accessible
            //    cclist_accessible
            //    estimated_time
            //    remaining_time
            //    alias
            //    product_id
            //    component_id
            //    deadline

            val bugData = BugData(-1, Integer.valueOf(bug_id), "<>", resolution, -1, -1, -1, "<>", null)

            println(bugData, assigned_to)
          }
        }
      }
    }
  }


  def toPDTDate(fullDate: String): DateTime = {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val timeZone = fullDate.substring(fullDate.length - 3)
    val actualDate = fullDate.substring(0, fullDate.length - 4)
    val dt = formatter.withZone(DateTimeZone.forID("PST8PDT")).parseDateTime(actualDate)
    dt
  }
}
