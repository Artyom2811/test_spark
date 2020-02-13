import SimpleApp.{connector, keyspace, sparkSession}
import models.{Movie, Rating}
import org.apache.spark.sql
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.Serialization
import JSONParsers.parseJson

class KafkaService {

  private val kassandraSevrice = new KassandraSevrice
  private val numberOfPartitions = 3

  private val writer: ForeachWriter[(String, String, Int, Long)] = new ForeachWriter[(String, String, Int, Long)] {
    override def open(partitionId: Long, version: Long) = true

    override def process(value: (String, String, Int, Long)): Unit = {
      val (topic, json, partition, offset) = value

      implicit val formats: Formats = Serialization.formats(NoTypeHints)

      connector.withSessionDo {
        session =>
          topic match {
            case "rating" =>
              val rating = parseJson[Rating](json)
              session execute s"""INSERT INTO $keyspace.ratings (userId, movieId, rating, timestamps) VALUES (${rating.userId}, ${rating.movieId}, ${rating.rating}, ${rating.timestamps + "000"});"""
            case "movies" =>
              val movie = parseJson[Movie](json)
              //Escaping single-quotes for cassandra
              val normalizedJson = movie.title replaceAll ("'", "''")
              session execute s"""INSERT INTO $keyspace.movies (movieId, title, genres) VALUES (${movie.movieId.toLong}, '$normalizedJson', '${movie.genres}');"""
          }
          session execute s"""INSERT INTO $keyspace.number_of_offset (topic, partition, offset) VALUES ('$topic', $partition, $offset);"""
      }
    }

    override def close(errorOrNull: Throwable): Unit = {}
  }

  import sparkSession.implicits._

  private def downloadFromKafka(topic: Array[String]): sql.DataFrame = sparkSession
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", topic.mkString(","))
    .option("startingOffsets", getJsonOffsets(topic, numberOfPartitions))
    .load
    .withColumn("topic", $"topic" cast StringType)
    .withColumn("value", $"value" cast StringType)
    .withColumn("partition", $"partition" cast IntegerType)
    .withColumn("offset", $"offset" cast LongType)
    .select("topic", "value", "partition", "offset")

  def downloadFromRatingKafka(topic: Array[String]): Unit =
    downloadFromKafka(topic)
      .as[(String, String, Int, Long)]
      .writeStream
      .trigger(Trigger.Once)
      .foreach(writer)
      .start
      .awaitTermination



  private def getJsonOffsets(topics: Array[String], numberOfPartitions: Int): String = {
    val offsetsOfRating: Array[(String, Int, Long)] = kassandraSevrice.getOffsetByTopicFromCassandra(topics)
    if (offsetsOfRating.nonEmpty) {
      val expectedOfPartitions = 0 until numberOfPartitions

      val offsetsByTopicAndPartition: Array[String] = topics.map {
        targetTopic =>

          val offsetsByTopic = offsetsOfRating
            .filter { case (topic, _, _) =>
              targetTopic == topic
            }

          val lines = expectedOfPartitions.map {
            targetPartition =>
              val offsets = offsetsByTopic
                .filter { case (_, partition, _) =>
                  partition == targetPartition
                }

              s""""$targetPartition":${offsets.headOption.map(_._3).getOrElse(-2)}"""
          }.toArray

          val resultPartitionsAndOffsets = lines.mkString(", ")

          s""""$targetTopic": {$resultPartitionsAndOffsets} """
      }

      s"""{${offsetsByTopicAndPartition.mkString(",")}}"""
    } else "earliest"
  }

}
