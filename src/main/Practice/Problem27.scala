/*
Problem27: You are tasked with analyzing a dataset containing user activity logs to derive meaningful insights that can inform business decisions and improve user experience. The dataset includes logs of user activities with timestamps. Your goal is to perform various analyses to understand user behavior, system usage patterns, and activity trends.

**Dataset:**
The dataset is in CSV format with the following columns:
- `userId`: Identifier for the user.
- `activity`: Type of activity performed by the user (e.g., login, purchase).
- `timestamp`: Date and time when the activity occurred.

**Sample Data:**
```plaintext
userId,activity,timestamp
user1,login,2024-08-01 08:00:00
user2,purchase,2024-08-01 09:15:30
user3,login,2024-08-01 10:00:00
user1,logout,2024-08-01 10:30:00
user4,purchase,2024-08-01 11:00:00
user2,login,2024-08-01 12:00:00
user1,purchase,2024-08-01 12:30:00
user3,logout,2024-08-01 13:00:00
user5,login,2024-08-01 13:30:00
user4,login,2024-08-01 14:00:00
user2,purchase,2024-08-01 15:00:00
user5,logout,2024-08-01 15:30:00
```

**Tasks:**

1. **Daily Active Users (DAU):**
   - Calculate the number of unique users who performed any activity on each day. This will help in understanding user engagement on a daily basis.

2. **Activity Frequency:**
   - Determine how frequently each type of activity (e.g., login, purchase) occurs in the dataset. This provides insight into which activities are most common.

3. **User Session Duration:**
   - Compute the duration of each user’s session by calculating the time between login and logout activities. This will help measure how long users interact with the system in each session.

4. **Activity Sequence:**
   - Analyze the sequence of activities for each user to understand common patterns or workflows. This will help in identifying typical user journeys.

5. **Peak Activity Times:**
   - Identify the times of day when activities are most frequent. This can inform scheduling and resource allocation to handle peak loads.

6. **Conversion Rate:**
   - Calculate the rate at which logins lead to purchases for each user. This will help in evaluating the effectiveness of user engagement strategies in converting logins into purchases.

**Expected Deliverables:**
- DataFrame or tables showing daily active users.
- Frequency counts of each type of activity.
- Duration of sessions for each user.
- Sequences of activities for each user.
- Activity counts by hour to identify peak times.
- Conversion rates from login to purchase for each user.

**Requirements:**
- Use Apache Spark with Scala to perform the data processing and analysis.
- The results should be presented in a clear and structured format.
- Ensure the code is well-documented and includes comments explaining each step of the analysis.

**Assumptions:**
- Each user has a unique identifier.
- Each activity is logged with an accurate timestamp.
- Logins and purchases are discrete events with clear start and end times.

**Constraints:**
- The dataset may contain large volumes of data; efficiency in processing is required.
- The analysis should be performed within a reasonable timeframe.
*/
import org.apache.spark.SparkContext.getOrCreate
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Problem27 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("UserDataAnalysis")
      .master("local[*]")
      .getOrCreate()

    val sampleData = Seq(
      ("user1", "login", "2024-08-01 08:00:00"),
      ("user2", "purchase", "2024-08-01 09:15:30"),
      ("user3", "login", "2024-08-01 10:00:00"),
      ("user1", "logout", "2024-08-01 10:30:00"),
      ("user4", "purchase", "2024-08-01 11:00:00"),
      ("user2", "login", "2024-08-01 12:00:00"),
      ("user1", "purchase", "2024-08-01 12:30:00"),
      ("user3", "logout", "2024-08-01 13:00:00"),
      ("user5", "login", "2024-08-01 13:30:00"),
      ("user4", "login", "2024-08-01 14:00:00"),
      ("user2", "purchase", "2024-08-01 15:00:00"),
      ("user5", "logout", "2024-08-01 15:30:00")
    )

    val dataDF = spark.createDataFrame(sampleData).toDF("UserID", "activity", "timeStamp")
    dataDF.show(false)

    val df = dataDF.withColumn("timeStamp", to_timestamp(col("timeStamp"),"yyyy-MM-dd HH:mm:ss"))
    df.show(false)

    // Daily Active users
    val activeUsersDF = df.groupBy(date_format(col("timeStamp"), "yyyy-MM-dd").as("date"))
      .agg(countDistinct("userId").as("activeUsers"))
    activeUsersDF.show()

    //Activity Frequency
    val activityFreqDF = df.groupBy("activity")
      .agg(count("*").as("Frequency"))
    activityFreqDF.show()

    //User Session Duration

    val sessiondurationDF = df.withColumn("session", lag("activity", 1).over(Window.partitionBy("userId").orderBy("timestamp")))
      .filter(col("activity") === "logout")
      .withColumn("loginTimestamp", lag("timestamp", 1).over(Window.partitionBy("userId").orderBy("timestamp")))
      .withColumn("sessionDuration", unix_timestamp(col("timestamp")) - unix_timestamp(col("loginTimestamp")))
      .select("userId", "loginTimestamp", "timestamp", "sessionDuration")

    sessiondurationDF.show()

    //Activity Sequence
    val activityseqDF = df.withColumn("sequence", row_number().over(Window.partitionBy("userId").orderBy("timeStamp")))
      .select("userId", "timeStamp", "activity", "sequence")
      .orderBy("userID", "sequence")

    activityseqDF.show()

    //Peak Activity Times
    val peakActivityTimes = df.withColumn("hour", hour(col("timestamp")))
      .groupBy("hour")
      .agg(count("*").as("activityCount"))
      .orderBy(desc("activityCount"))

    peakActivityTimes.show()

    // Conversion Rate
    val loginPurchases = df
      .filter(col("activity") === "login")
      .join(df.filter(col("activity") === "purchase"), "userId")
      .groupBy("userId")
      .agg(countDistinct("timestamp").as("logins"), countDistinct("timestamp").as("purchases"))
      .withColumn("conversionRate", col("purchases") / col("logins"))

    loginPurchases.show()

    // Stop the Spark session
    spark.stop()
  }
}
