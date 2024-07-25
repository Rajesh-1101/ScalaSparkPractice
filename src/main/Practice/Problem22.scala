/*
Problem22: Given a dataset with columns PERSON, TYPE, and AGE,
create an output where the oldest adult is paired with the youngest child,
producing pairs of ADULT and CHILD while ensuring appropriate data matching.

Check out the input and output in the table below!

Input:--->

| PERSON | TYPE | AGE |
| ------ | ------ | --- |
| A1 | ADULT | 54 |
| A2 | ADULT | 53 |
| A3 | ADULT | 52 |
| A4 | ADULT | 58 |
| A5 | ADULT | 54 |
| C1 | CHILD | 20 |
| C2 | CHILD | 19 |
| C3 | CHILD | 22 |
| C4 | CHILD | 15 |


Expected Output:--->

| ADULT | CHILD |
| ----- | ----- |
|   A1|   C2|
|   A2|   C3|
|   A3| NULL|
|   A4|   C4|
|   A5|   C1|
*/
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Problem22 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Problem 22")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Sample data
    val data = Seq(
      ("A1", "ADULT", 54),
      ("A2", "ADULT", 53),
      ("A3", "ADULT", 52),
      ("A4", "ADULT", 58),
      ("A5", "ADULT", 54),
      ("C1", "CHILD", 20),
      ("C2", "CHILD", 19),
      ("C3", "CHILD", 22),
      ("C4", "CHILD", 15)
    )

    // Create DataFrame
    val dataDF = data.toDF("Person", "Type", "Age")
    dataDF.show()

    // Rank adults by descending age
    val windowSpecAdult = Window.partitionBy("Type").orderBy(desc("Age"))
    val adultRankedDF = dataDF.filter($"Type" === "ADULT")
      .withColumn("rank", row_number().over(windowSpecAdult))
      .select($"Person".alias("ADULT"), $"Age", $"rank")
    adultRankedDF.show()

    // Rank children by ascending age
    val windowSpecChild = Window.partitionBy("Type").orderBy("Age")
    val childRankedDF = dataDF.filter($"Type" === "CHILD")
      .withColumn("rank", row_number().over(windowSpecChild))
      .select($"Person".alias("CHILD"), $"Age", $"rank")
    childRankedDF.show()

    // Join adultRankedDF with childRankedDF based on their ranks
    val resultDF = adultRankedDF.join(childRankedDF, Seq("rank"), "left")
      .select($"ADULT", $"CHILD")
      .orderBy($"ADULT".asc_nulls_last)

    resultDF.show()

    spark.stop()
  }
}
