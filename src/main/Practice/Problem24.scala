/*
Problem24: What are the different ways to handle row duplication in a Scala_Spark data frame?

There are two ways to handle row duplication in data frames. The distinct() function
in spark is used to drop/remove duplicate rows (all columns) from a DataFrame,
while dropDuplicates() is used to drop rows based on one or more columns.
*/
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Problem24 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Problem24")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Sample data
    val dataDF = Seq(
      ("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    ).toDF("employee_name", "department", "salary")
    dataDF.show(false)

    //distinct value
    val distinctDF = dataDF.distinct()
    distinctDF.show(false)

    //drop duplicates
    val dropDuplicateDF = dataDF.dropDuplicates()
    dropDuplicateDF.show(false)

    //drop duplicates on selected columns
    val dropSelDF = dataDF.dropDuplicates("department", "salary")
    dropSelDF.show(false)

    spark.stop()
  }
}
