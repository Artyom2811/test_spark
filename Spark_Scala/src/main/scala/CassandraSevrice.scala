import SimpleApp.sparkSession
import models.{Movie, Rating}
import org.apache.spark.sql.{Dataset, SaveMode}
import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.Serialization
import org.apache.spark.sql.cassandra._
import sparkSession.implicits._
import org.apache.spark.sql.functions._

class CassandraSevrice {

  val keyspace = "testkeyspace"

  def writeToCassandra[T](keyspace: String,
                          tableName: String,
                          dataset: Dataset[T]): Unit = dataset
    .write
    .option("confirm.truncate", value = true)
    .mode(SaveMode.Append)
    .cassandraFormat(tableName, keyspace)
    .save

  def readFromCassandraRatings(): Dataset[Rating] = {
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    sparkSession
      .read
      .cassandraFormat("ratings", keyspace)
      .load
      .select("userId", "movieId", "rating", "timestamps")
      .collect()
      .map(e => Rating(e.getLong(0), e.getLong(1), e.getDouble(2), e.getTimestamp(3).getTime))
      .toList
      .toDS
  }

  def readFromCassandraMovies(): Dataset[Movie] = {
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    sparkSession
      .read
      .cassandraFormat("movies", keyspace)
      .load
      .select("movieId", "title", "genres")
      .collect()
      .map(e => Movie(e.getLong(0).toString, e.getString(1), e.getString(2)))
      .toList
      .toDS
  }

  def getOffsetByTopicFromCassandra(topics: Array[String]): Array[(String, Int, Long)] = sparkSession
    .read
    .cassandraFormat("number_of_offset", keyspace)
    .load
    .filter(col("topic") isin (topics: _*))
    .select("topic", "partition", "offset")
    .as[(String, Int, Long)]
    .collect

  def getOffsetByAllTopicsFromCassandra: Array[(String, Int, Long)] = sparkSession
    .read
    .cassandraFormat("number_of_offset", keyspace)
    .load
    .select("topic", "partition", "offset")
    .as[(String, Int, Long)]
    .collect
}
