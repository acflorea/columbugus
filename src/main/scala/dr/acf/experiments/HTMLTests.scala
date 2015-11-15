package dr.acf.experiments

import java.io.File

import com.sun.xml.bind.v2.runtime.unmarshaller.TagName
import org.htmlcleaner.{TagNode, HtmlCleaner}

/**
  * Created by aflorea on 15.11.2015.
  */
object HTMLTests {
  def main(args: Array[String]) {

    val url = "./src/main/resources/dummy-test-files/212539.html"

    val cleaner = new HtmlCleaner()
    val props = cleaner.getProperties
    val rootNode = cleaner.clean(new File(url))

    // Bug information columns
    val changeForm = rootNode.findElementByAttValue("name", "changeform", true, true)
    val bz_show_bug_column_1 = changeForm.findElementByAttValue("id", "bz_show_bug_column_1", true, true)
    val bz_show_bug_column_2 = changeForm.findElementByAttValue("id", "bz_show_bug_column_2", true, true)

    //    bug_id
    val bug_id = changeForm.findElementByAttValue("name", "id", true, true).getAttributeByName("value")
    //    assigned_to

    //    bug_file_loc
    //    bug_severity
    //    bug_status
    //    creation_ts
    //    delta_ts
    //    short_desc
    //    op_sys
    //    priority
    //    rep_platform
    //    reporter
    //    version
    //    resolution
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

    println("Done!")
  }


}
