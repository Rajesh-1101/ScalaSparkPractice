/*
question: Table called 'Logs'. The table contains two columns: 'id' and 'num'.
Your task is to find all numbers that appear at least three times consecutively.
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lag, lead}

object ConsecutiveNumbers {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ConsecutiveNumbers")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      (1, 5), (2, 5), (3, 5), (4, 6), (5, 6),
      (6, 6), (7, 7), (8, 6), (9, 6), (10, 6)
    )
    val logsDF = data.toDF("id", "num")

    val windowSpec = Window.orderBy("id")

    val logs = logsDF
      .withColumn("prevNum", lag("num", 1).over(windowSpec))
      .withColumn("nextNum", lead("num", 1).over(windowSpec))

    val consecutiveDF = logs
      .filter($"num" === $"prevNum" && $"num" === $"nextNum")
      .select("num")
      .distinct()

    consecutiveDF.show()

    spark.stop()
  }
}
