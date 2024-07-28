/*
Problem25: While creating a Scala Spark DataFrame we can specify the structure using StructType
and StructField classes. As specified in the introduction, StructType is a collection of
StructFields which is used to define the column name, data type, and a flag for nullable
or not. Using StructField we can also add nested struct schema, ArrayType for arrays,
and MapType for key-value pairs which we will discuss in detail in later sections.

The below example demonstrates a very simple example of how to create a StructType &
StructField on DataFrame and its usage with sample data to support it.

data = [("James","","Smith","36636","M",3000),
 ("Michael","Rose","","40288","M",4000),
 ("Robert","","Williams","42114","M",4000),
 ("Maria","Anne","Jones","39192","F",4000),
 ("Jen","Mary","Brown","","F",-1)
 ]
*/
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._

object Problem25 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Problem 25")
      .master("local[*]")  // Use all available cores
      .getOrCreate()

    import spark.implicits._

    // Sample data
    val data = Seq(
      ("James", "", "Smith", "36636", "M", 3000),
      ("Michael", "Rose", "", "40288", "M", 4000),
      ("Robert", "", "Williams", "42114", "M", 4000),
      ("Maria", "Anne", "Jones", "39192", "F", 4000),
      ("Jen", "Mary", "Brown", "", "F", -1)
    )

    // Define schema
    val schema = StructType(Array(
      StructField("first_name", StringType, nullable = true),
      StructField("middle_name", StringType, nullable = true),
      StructField("last_name", StringType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("gender", StringType, nullable = true),
      StructField("salary", IntegerType, nullable = true)
    ))

    // Convert tuples to Row objects
    val rowRDD = spark.sparkContext.parallelize(data.map {
      case (first_name, middle_name, last_name, id, gender, salary) =>
        Row(first_name, middle_name, last_name, id, gender, salary)
    })

    // Create DataFrame with schema
    val dataDF = spark.createDataFrame(rowRDD, schema)

    // Show DataFrame
    dataDF.show()
  }
}
