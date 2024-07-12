/*
Write an Pyspark code to find the names of all employees and the name of their immediate boss. 
If an employee does not have a boss, display "No Boss" for them.
Sample data
data = [
    (1, "Alice", None),
    (2, "Bob", 1),
    (3, "Carol", 2),
    (4, "Dave", 1),
    (5, "Eve", 2),
    (6, "Frank", 4)
]

schema = ["ID", "Name", "Boss"]
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, lit}

object EmployeeBoss {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("EmployeeBoss")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    
    val data = Seq(
      (1, "Alice", None),
      (2, "Bob", Some(1)),
      (3, "Carol", Some(2)),
      (4, "Dave", Some(1)),
      (5, "Eve", Some(2)),
      (6, "Frank", Some(4))
    )
    
    val schema = List("ID", "Name", "Boss")
    val df = data.toDF(schema: _*)
    
    val joinedDF = df.as("emp")
      .join(df.as("boss"), $"emp.Boss" === $"boss.ID", "left_outer")
      .select($"emp.Name".as("Employee"), coalesce($"boss.Name", lit("No Boss")).as("Boss"))
    
    joinedDF.show()
    
    spark.stop()
  }
}
