/*
Problem26: What is meant by scala Spark MapType? How can you create a MapType using
StructType?

scala Spark MapType accepts two mandatory parameters- keyType and valueType, and one
optional boolean argument valueContainsNull.
Here’s how to create a MapType with scala Spark StructType and StructField. The
StructType() accepts a list of StructFields, each of which takes a field name and a value
type.
*/
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._

object Problem26 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Problem 26")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      ("James",Map("hair" -> "black", "eye"->"brown")),
      ("Michael",Map("hair"->"brown","eye"->null.asInstanceOf[String])),
      ("Robert",Map("hair"->"red","eye"->"black")),
      ("Washington",Map("hair"->"grey","eye"->"grey")),
      ("Jefferson",Map("hair"->"brown","eye"->""))
    )

    val schema = StructType(Array(
      StructField("first_name", StringType, nullable = true),
      StructField("attributes", MapType(StringType, StringType, valueContainsNull = true), nullable = true)
    ))

    val rowRDD = spark.sparkContext.parallelize(data.map {
      case (first_name, attributes) =>
        Row(first_name, attributes)
    })

    val dataDF = spark.createDataFrame(rowRDD, schema)

    dataDF.show()
  }
}
