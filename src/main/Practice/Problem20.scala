/*
Problem20: The given file has a delimiter ~|. How will you load it as a spark DataFrame?

Important: Instead of using sparkContext(sc), use sparkSession (spark).
Name ~|Age
Azarudeen, Shahul~|25
Michel, Clarke ~|26
Virat, Kohli ~|28
Andrew, Simond ~|37
George, Bush~|59
Flintoff, David ~|12
*/

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window => ExprWindow}
import org.apache.spark.sql.catalyst.plans.logical.{Window => LogicalWindow}

object customDelimeter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("custom Delimeter")
      .master("local[*]")
      .getOrCreate()

    val data = Seq(
      "Azarudeen, Shahul~|25",
      "Michel, Clarke ~|26",
      "Virat, Kohli ~|28",
      "Andrew, Simond ~|37",
      "George, Bush~|59",
      "Flintoff, David ~|12"
    )

    val schema = StructType(
      Array(
        StructField("Name", StringType, nullable = true),
        StructField("Age", IntegerType, nullable = true)
      )
    )
    import spark.implicits._
    val dataDF = spark.sparkContext.parallelize(data)
      .map(_.split("~\\|"))
      .map(row => (row(0), row(1).toInt))
      .toDF("Name", "Age")

    dataDF.show()
    spark.stop()
  }
}
