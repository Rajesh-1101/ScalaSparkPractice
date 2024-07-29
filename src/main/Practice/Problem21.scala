/*
Problem21: Find the origin and the destination of each customer.

Note: If there is a discontinuity in the flight's origin & destination,
it will be a new route (example cust_id 4).

Input-
cust_id, flight_id, origin, destination
(1, 'Flight2', 'Goa', 'Kochi'),
(1, 'Flight1', 'Delhi', 'Goa'),
(1, 'Flight3', 'Kochi', 'Hyderabad'),
(2, 'Flight1', 'Pune', 'Chennai'),
(2, 'Flight2', 'Chennai', 'Pune'),
(3, 'Flight1', 'Mumbai', 'Bangalore'),
(3, 'Flight2', 'Bangalore', 'Ayodhya'),
(4, 'Flight1', 'Ahmedabad', 'Indore'),
(4, 'Flight2', 'Indore', 'Kolkata'),
(4, 'Flight3', 'Ranchi', 'Delhi'),
(4, 'Flight4', 'Delhi', 'Mumbai')

Output-
|cust_id|  origin|destination|
+-------+---------+-----------+
|   1|  Delhi| Hyderabad|
|   2|   Pune|    Pune|
|   3|  Mumbai|  Ayodhya|
|   4|Ahmedabad|  Kolkata|
|   4|Ranchi |   Mumbai|
*/

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object flightsData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("flights Data")
      .master("local[*]")
      .getOrCreate()

    val flights = Seq(
      (1, "Flight2", "Goa", "Kochi"),
      (1, "Flight1", "Delhi", "Goa"),
      (1, "Flight3", "Kochi", "Hyderabad"),
      (2, "Flight1", "Pune", "Chennai"),
      (2, "Flight2", "Chennai", "Pune"),
      (3, "Flight1", "Mumbai", "Bangalore"),
      (3, "Flight2", "Bangalore", "Ayodhya"),
      (4, "Flight1", "Ahmedabad", "Indore"),
      (4, "Flight2", "Indore", "Kolkata"),
      (4, "Flight3", "Ranchi", "Delhi"),
      (4, "Flight4", "Delhi", "Mumbai")
    )

    import spark.implicits._
    val flightsDF = flights.toDF("cust_id", "flight_id", "origin", "destination")

    flightsDF.show()

    val originDestinationDF = flightsDF.groupBy("cust_id")
      .agg(
        first("origin").as("origin_start"),
        last("destination").as("destination_end")
      )
    originDestinationDF.show()
    spark.stop()
  }
}
