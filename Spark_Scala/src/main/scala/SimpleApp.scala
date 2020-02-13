
import com.datastax.spark.connector.cql.CassandraConnector
import models.{GenreRatingByDayEntry, Movie, Rating}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf

object SimpleApp {
  val conf: SparkConf = new SparkConf().setMaster("local")
    .setAppName("Simple Application")
    .set("spark.cassandra.connection.connections_per_executor_max", "5")

  val connector: CassandraConnector = CassandraConnector(conf)

  val sparkSession: SparkSession = SparkSession.builder.config(conf).getOrCreate

  val keyspace = "testkeyspace"
  val kassandraSevrice = new KassandraSevrice
  val nameTopicRating = "rating"
  val nameTopicMovies = "movies"
  val reportTable = "count_stars_by_day"

  import sparkSession.implicits._

  def main(args: Array[String]) {
    var offsets = kassandraSevrice.getOffsetByAllTopicsFromCassandra

    while (true) {
      //Starts consumption from kafka and waits until the end
      val kafkaService = new KafkaService
      kafkaService.downloadFromRatingKafka(Array(nameTopicRating, nameTopicMovies))

      if (!(offsets sameElements kassandraSevrice.getOffsetByAllTopicsFromCassandra)) {
        println("Calculating data")
        val result: Dataset[GenreRatingByDayEntry] = calcReportCountStarsByDay

        kassandraSevrice.writeToCassandra(keyspace, reportTable, result)
        offsets = kassandraSevrice.getOffsetByAllTopicsFromCassandra
      }
      println("Loop is over")
      Thread.sleep(10000)
    }
  }

  def calcReportCountStarsByDay: Dataset[GenreRatingByDayEntry] = {
    val listOfMovies: Dataset[Movie] = kassandraSevrice.readFromCassandraMovies
    val listOfRatings: Dataset[Rating] = kassandraSevrice.readFromCassandraRatings

    val countRowByGenreAndDateAndRating = listOfRatings
      .withColumn("date", to_date(col("timestamps") cast TimestampType))
      .join(listOfMovies, "movieId")
      .withColumn("genre", explode(split(col("genres"), "\\|")))
      .withColumn("rating", floor(col("rating")))
      .groupBy("genre", "date", "rating").count

    val countStarsByGenreAndDate = countRowByGenreAndDateAndRating
      .withColumn("one_star_count", when(col("rating") === 1, col("count")).otherwise(0))
      .withColumn("two_star_count", when(col("rating") === 2, col("count")).otherwise(0))
      .withColumn("three_star_count", when(col("rating") === 3, col("count")).otherwise(0))
      .withColumn("four_star_count", when(col("rating") === 4, col("count")).otherwise(0))
      .withColumn("five_star_count", when(col("rating") === 5, col("count")).otherwise(0))
      .groupBy("genre", "date")
      .agg(
        sum("one_star_count").alias("one_star_count"),
        sum("two_star_count").alias("two_star_count"),
        sum("three_star_count").alias("three_star_count"),
        sum("four_star_count").alias("four_star_count"),
        sum("five_star_count").alias("five_star_count")
      )

    val resultReportCountStarsByDay = countStarsByGenreAndDate
      .withColumn("day", dayofmonth(col("date")))
      .withColumn("year_month", date_format(col("date"), "MMyyyy").cast("Long"))
      .orderBy("date", "genre")
      .drop(col("date"))
      .as[GenreRatingByDayEntry]

    resultReportCountStarsByDay
  }
}