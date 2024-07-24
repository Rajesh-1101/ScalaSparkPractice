/*
Problem22: Given a dataset with columns PERSON, TYPE, and AGE,
create an output where the oldest adult is paired with the youngest child,
producing pairs of ADULT and CHILD while ensuring appropriate data matching.

💡 Check out the input and output in the table below!

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
| A4 | C4 |
| A5 | C2 |
| A1 | C1 |
| A2 | C3 |
| A3 | NULL |
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

    val dataDF = data.toDF("Person", "Type", "Age")
    dataDF.show()

    val windowSpecAdult = Window.partitionBy("Type").orderBy(desc("Age"))
    val adultRankedDF = dataDF.filter($"Type" === "ADULT")
      .withColumn("rank", rank().over(windowSpecAdult))
      .select($"Person".alias("ADULT"), $"Age",  $"rank")
    adultRankedDF.show()

    val windowSpecChild = Window.partitionBy("Type").orderBy("Age")
    val childRankedDF = dataDF.filter($"Type" === "CHILD")
      .withColumn("rank", rank().over(windowSpecChild))
      .select($"Person".alias("CHILD"), $"Age", $"rank")
    childRankedDF.show()

    val resultDF = adultRankedDF.join(childRankedDF, Seq("rank"), "left")
      .orderBy($"ADULT".asc_nulls_last)
      .select($"ADULT", $"CHILD")

    resultDF.show()

    spark.stop()
  }
}
