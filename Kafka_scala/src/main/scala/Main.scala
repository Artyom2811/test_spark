import java.io.File
import java.util.Properties

import org.apache.kafka.clients.producer._
import com.github.tototoshi.csv._
import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write

object Main {
  val props = new Properties
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("batch.size", "10000")

  val nameRatingTopic = "rating"
  val nameMoviesTopic = "movies"

  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

    def main(args: Array[String]): Unit = {
      val csvReaderRatings = readFromCSV("ratings.csv")
      val arrayOfRating = parseCsvToRatings(csvReaderRatings)
      val ratingsJson = arrayOfRating map ratingToJson

      sendArrayToKafka(nameRatingTopic, ratingsJson)

//      val csvReaderMovies = readFromCSV("movies.csv")
//      val arrayOfMovies = parseCsvToMovies(csvReaderMovies)
//      val moviesJson = arrayOfMovies map movieToJson
//
//      sendArrayToKafka(nameMoviesTopic, moviesJson)
    }

  def sendArrayToKafka(topic: String, ratings: Array[String]) {
    ratings
      .sliding(200, 200)
      .foreach {
        ratings =>
          ratings.foreach(rating => writeToKafka(topic, rating))
          Thread.sleep(10000)
      }
  }

  def writeToKafka(topic: String, message: String): Unit = {
    println("Send data to Kafka:" + message)

    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topic, null, message)
    producer.send(record)
    producer.close
  }

  def readFromCSV(path: String): Stream[Map[String, String]] = CSVReader
    .open(new File(path))
    .toStreamWithHeaders

  def parseCsvToRatings(ratings: Stream[Map[String, String]]): Array[Rating] = ratings
    .map(e => Rating(e("userId").toString, e("movieId").toString, e("rating").toDouble, e("timestamps").toString))
    .toArray

  def parseCsvToMovies(movies: Stream[Map[String, String]]): Array[Movie] = movies
    .map(e => Movie(e("movieId"), e("title"), e("genres")))
    .toArray

  def ratingToJson(rating: Rating)(implicit formats: Formats): String = write(rating)

  def movieToJson(movie: Movie)(implicit formats: Formats): String = write(movie)
}