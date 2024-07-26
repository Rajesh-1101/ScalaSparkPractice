/*
Problem22: Write a solution to display the records with three or more rows with
consecutive IDs, and the number of people is greater than or equal to 100 for each.
Return the result table ordered by visit_date in ascending order.

Table: Stadium
+---------------+---------+
| Column Name  | Type  |
+---------------+---------+
| id      | int   |
| visit_date  | date  |
| people    | int   |
+---------------+---------+
visit_date is the column with unique values for this table.
Each row of this table contains the visit date and visit id to the stadium with
the number of people during the visit.
As the ID increases, the date increases as well.
---------------------------------------------------------------------------------------------------
Input:
Stadium table:
+------+------------+-----------+
| id  | visit_date | people  |
+------+------------+-----------+
| 1  | 2017-01-01 | 10    |
| 2  | 2017-01-02 | 109    |
| 3  | 2017-01-03 | 150    |
| 4  | 2017-01-04 | 99    |
| 5  | 2017-01-05 | 145    |
| 6  | 2017-01-06 | 1455   |
| 7  | 2017-01-07 | 199    |
| 8  | 2017-01-09 | 188    |
+------+------------+-----------+

Output:
+------+------------+-----------+
| id  | visit_date | people  |
+------+------------+-----------+
| 5  | 2017-01-05 | 145    |
| 6  | 2017-01-06 | 1455   |
| 7  | 2017-01-07 | 199    |
| 8  | 2017-01-09 | 188    |
+------+------------+-----------+

Explanation:
The four rows with ids 5, 6, 7, and 8 have consecutive ids and each of them
has >= 100 people attended. Note that row 8 was included even though the visit_date
was not the next day after row 7.
The rows with ids 2 and 3 are not included because we need at least three consecutive ids.
*/
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Problem23 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Problem23")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Sample data
    val dataDF = Seq(
      (1, "2017-01-01", 10),
      (2, "2017-01-02", 109),
      (3, "2017-01-03", 150),
      (4, "2017-01-04", 99),
      (5, "2017-01-05", 145),
      (6, "2017-01-06", 1455),
      (7, "2017-01-07", 199),
      (8, "2017-01-09", 188)
    ).toDF("id", "visit_date", "people")
    dataDF.show()

  // Define a window specification ordered by id
    val windowSpec = Window.orderBy("id")

    // Filter df where people >= 100
    val filteredDf = dataDF.filter($"people" >= 100)
      .withColumn("rnum", row_number().over(windowSpec))
      .withColumn("diff", $"id" - $"rnum")
    filteredDf.show()

    // Grouping the data based on diff and counting occurrences
    val groupedDf = filteredDf.groupBy("diff").count()

    // Join filtered DataFrame with grouped results
    val joinedDf = filteredDf.join(groupedDf, Seq("diff"), "inner")
    joinedDf.show()

    // Filter where count of diff >= 3 (i.e., at least 3 consecutive ids)
    val resultDf = joinedDf.filter($"count" >= 3)
      .drop("diff")
      .drop("rnum")
      .drop("count")
      .orderBy("id")

    resultDf.show()

    spark.stop()
  }
}
