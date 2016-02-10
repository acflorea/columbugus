package dr.acf.connectors

import java.sql.Timestamp

import slick.driver.MySQLDriver.api._


/**
  * Created by aflorea on 07.02.2016.
  */
trait SlickConnector {

  val db = Database.forConfig("mySQLBugsDB")

  // ok-ish
  val bugs = TableQuery[Bugs]
  // OK
  val bug_activities = TableQuery[Bugs_Activity]

  val longdescs = TableQuery[Longdescs]
  // OK
  val duplicates = TableQuery[Duplicates]
  // OK
  val components = TableQuery[Components]
  // OK
  val products = TableQuery[Products]
  // OK
  val assignments = TableQuery[Assignments]
  // OK
  val fields = TableQuery[Fields]

}

/**
  * Assignments
  *
  * @param tag
  */
class Assignments(tag: Tag) extends Table[(Int, String)](tag, "assignments") {
  def assignment_id = column[Int]("assignment_id", O.PrimaryKey)

  def assignment_name = column[String]("assignment_name")

  def * = (assignment_id, assignment_name)
}

/**
  * Fields
  *
  * @param tag
  */
class Fields(tag: Tag) extends Table[(Int, String)](tag, "fielddefs") {
  def field_id = column[Int]("id", O.PrimaryKey)

  def field_name = column[String]("name")

  def * = (field_id, field_name)
}


/**
  * Bug core
  *
  * @param tag
  */
class Bugs(tag: Tag)
  extends Table[
    (Int,
      Timestamp,
      String,
      String,
      Int,
      Int,
      String,
      String,
      Timestamp)
    ](tag, "bugs") {
  def bug_id = column[Int]("bug_id", O.PrimaryKey)

  def creation_ts = column[Timestamp]("creation_ts")

  def short_desc = column[String]("short_desc")

  def bug_status = column[String]("bug_status")

  def assigned_to = column[Int]("assigned_to")

  def component_id = column[Int]("component_id")

  def bug_severity = column[String]("bug_severity")

  def resolution = column[String]("resolution")

  def delta_ts = column[Timestamp]("delta_ts")

  def * = (
    bug_id,
    creation_ts,
    short_desc,
    bug_status,
    assigned_to,
    component_id,
    bug_severity,
    resolution,
    delta_ts)
}

/**
  * Bug Activity
  *
  * @param tag
  */
class Bugs_Activity(tag: Tag) extends Table[(Int, Int, Int, Timestamp, String, String)](tag, "bugs_activity") {

  def bug_id = column[Int]("bug_id")

  def who = column[Int]("who")

  def fieldid = column[Int]("fieldid")

  def bug_when = column[Timestamp]("bug_when")

  def added = column[String]("added")

  def removed = column[String]("removed")

  def * = (bug_id, who, fieldid, bug_when, added, removed)
}

/**
  * Long description - full desc and comments
  *
  * @param tag
  */
class Longdescs(tag: Tag) extends Table[(Int, Int, Timestamp, String)](tag, "longdescs") {
  def comment_id = column[Int]("comment_id", O.PrimaryKey)

  def bug_id = column[Int]("bug_id")

  def who = column[Int]("who")

  def bug_when = column[Timestamp]("bug_when")

  def thetext = column[String]("thetext")

  def * = (bug_id, who, bug_when, thetext)
}

/**
  * Duplicates
  *
  * @param tag
  */
class Duplicates(tag: Tag) extends Table[(Int, Int)](tag, "duplicates") {

  def dupe = column[Int]("dupe")

  def dupe_of = column[Int]("dupe_of")

  def * = (dupe, dupe_of)

}

/**
  * Components
  *
  * @param tag
  */
class Components(tag: Tag) extends Table[(Int, Int, String)](tag, "components") {

  def id = column[Int]("id", O.PrimaryKey)

  def product_id = column[Int]("product_id")

  def name = column[String]("name")

  def * = (id, product_id, name)
}

/**
  * Products
  *
  * @param tag
  */
class Products(tag: Tag) extends Table[(Int, Int, String)](tag, "products") {
  def id = column[Int]("id", O.PrimaryKey)

  def classification_id = column[Int]("classification_id")

  def name = column[String]("name")

  def * = (id, classification_id, name)
}
