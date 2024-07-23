/*
Problem19: How would you calculate the month-wise cumulative revenue using Spark?

Given data :
data = [(
'3000' , '22-may'),
('5000' , '23-may'),
('5000' , '25-may'),
('10000' , '22-june'),
('1250' , '03-july')]
schema = ['revenue','date']

here, we need to add one column for a month and then we can calculate the sum based
on a monthly basis.
*/

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window => ExprWindow}
import org.apache.spark.sql.catalyst.plans.logical.{Window => LogicalWindow}

object MonthlyRevenue {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Monthly Revenue")
      .master("local[*]")
      .getOrCreate()

    val data = Seq(
      ("3000", "22-may"),
      ("5000", "23-may"),
      ("5000", "25-may"),
      ("10000", "22-june"),
      ("1250", "03-july")
    )

    val schema = StructType(
      Seq(
        StructField("revenue", StringType, nullable = false),
        StructField("date", StringType, nullable = false)
      )
    )

    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(data)
    val dataDF = spark.createDataFrame(rdd).toDF("revenue", "date")
      .withColumn("revenue", col("revenue").cast(IntegerType))
    dataDF.show()

    val monthDF = dataDF.withColumn("month", substring(col("date"), 4, 3))
    monthDF.show()

    val windowSpec = ExprWindow.partitionBy("month").orderBy("date").rowsBetween(ExprWindow.unboundedPreceding, ExprWindow.currentRow)

    val resultDF = monthDF.withColumn("cumulative_revenue", sum("revenue").over(windowSpec))
    resultDF.show()

    spark.stop()
  }
}
