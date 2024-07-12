/*
Question: We have 3 table ( Sales Details, Store Details, and Customer Details). find out how many sales happened for the customers and store residing in same location/city.

Sample data for Sales_Details
sales_data = [
    (1, 101, "01-01-2020", 30),
    (1, 102, "02-01-2020", 50),
    (4, 101, "11-06-2020", 150),
    (5, 108, "13-06-2020", 20),
    (5, 104, "14-07-2020", 220),
    (6, 108, "15-08-2020", 190),
    (1, 103, "03-03-2020", 60),
    (2, 104, "04-03-2020", 100),
    (3, 101, "08-05-2020", 60),
    (3, 107, "09-05-2020", 120),
    (4, 103, "10-05-2020", 110),
    (7, 105, "18-09-2020", 40),
    (7, 102, "19-10-2020", 120),
    (1, 103, "03-06-2020", 120)
]

Sample data for Store Details
store_data = [
    (1, "STORE-1", "Chennai"),
    (2, "STORE-2", "Mumbai"),
    (3, "STORE-3", "Calcutta"),
    (4, "STORE-4", "New Delhi"),
    (5, "STORE-5", "Bangalore"),
    (8, "STORE-6", "Pune")
]

Sample data for Customer Details
customer_data = [
    (101, "Arun", "Chennai"),
    (102, "Vijay", "Mumbai"),
    (105, "Meera", "Kolkatta"),
    (106, "Rajesh", "New Delhi"),
    (107, "Sanjay", "New Delhi"),
    (108, "Ram", "Bangalore")
]
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SalesAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Sales Analysis")
      .getOrCreate()

    import spark.implicits._

    val sales_data = Seq(
      (1, 101, "01-01-2020", 30),
      (1, 102, "02-01-2020", 50),
      (4, 101, "11-06-2020", 150),
      (5, 108, "13-06-2020", 20),
      (5, 104, "14-07-2020", 220),
      (6, 108, "15-08-2020", 190),
      (1, 103, "03-03-2020", 60),
      (2, 104, "04-03-2020", 100),
      (3, 101, "08-05-2020", 60),
      (3, 107, "09-05-2020", 120),
      (4, 103, "10-05-2020", 110),
      (7, 105, "18-09-2020", 40),
      (7, 102, "19-10-2020", 120),
      (1, 103, "03-06-2020", 120)
    )
    val salesColumns = Seq("STORE_ID", "CUST_ID", "TRANSACTION_DATE", "SALES")
    val salesDF = spark.createDataFrame(sales_data).toDF(salesColumns: _*)

    val store_data = Seq(
      (1, "STORE-1", "Chennai"),
      (2, "STORE-2", "Mumbai"),
      (3, "STORE-3", "Calcutta"),
      (4, "STORE-4", "New Delhi"),
      (5, "STORE-5", "Bangalore"),
      (8, "STORE-6", "Pune")
    )
    val storeColumns = Seq("STORE_ID", "STORE_NAME", "LOCATION")
    val storeDF = spark.createDataFrame(store_data).toDF(storeColumns: _*)

    val customer_data = Seq(
      (101, "Arun", "Chennai"),
      (102, "Vijay", "Mumbai"),
      (105, "Meera", "Kolkata"),
      (106, "Rajesh", "New Delhi"),
      (107, "Sanjay", "New Delhi"),
      (108, "Ram", "Bangalore")
    )
    val customerColumns = Seq("CUST_ID", "CUST_NAME", "CUST_LOCATION")
    val customerDF = spark.createDataFrame(customer_data).toDF(customerColumns: _*)

    val joinedDF = salesDF
      .join(storeDF, Seq("STORE_ID"))
      .join(customerDF, Seq("CUST_ID"))
      .filter(col("STORE.LOCATION") === col("CUSTOMER.CUST_LOCATION"))
      .select("CUST_ID", "CUST_NAME", "STORE_ID", "STORE_NAME", "LOCATION", "SALES")

    val salesCountByLocation = joinedDF
      .groupBy("CUST_ID", "CUST_NAME", "STORE_ID", "STORE_NAME", "LOCATION")
      .count()
      .orderBy("CUST_ID")

    salesCountByLocation.show()

    spark.stop()
  }
}
