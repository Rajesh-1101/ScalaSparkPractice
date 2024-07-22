/*
Problem18: Write a spark program to report the movies with an odd-numbered ID and
a description that is not "boring". Return the result table in descending order by rating.

INPUT:
+---+----------+-----------+------+
| ID| movie|description|rating|
+---+----------+-----------+------+
| 1| War| great 3D| 8.9|
| 2| Science| fiction| 8.5|
| 3| irish| boring| 6.2|
| 4| Ice song| Fantacy| 8.6|
| 5|House card|Interesting| 9.1|
+---+----------+-----------+------+

OUTPUT:
+---+----------+-----------+------+
| ID| movie|description|rating|
+---+----------+-----------+------+
| 1| War| great 3D| 8.9|
| 5|House card|Interesting| 9.1|
+---+----------+-----------+------+
*/
import org.apache.spark.sql.SparkSession

object movieFilter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("movie Filter")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val moviesDataDF = Seq(
      (1, "War","great 3D",8.9),
      (2, "Science","fiction",8.5),
      (3, "irish","boring",6.2),
      (4, "Ice song","Fantacy",8.6),
      (5, "House card","Interesting",9.1)
    ).toDF("ID", "movie", "description", "rating")

    moviesDataDF.createOrReplaceTempView("movies")

    val result = spark.sql(
      """
        |SELECT *
        |FROM movies
        |WHERE MOD(ID,2) = 1
        |  AND
        |  lower(description) NOT like "%boring%"
        |ORDER BY rating DESC
        |""".stripMargin)

  result.show()
  spark.stop()
  }
}
