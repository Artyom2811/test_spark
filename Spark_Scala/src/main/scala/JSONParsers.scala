import org.json4s.JsonAST.{JInt, JString}
import org.json4s.{CustomSerializer, DefaultFormats}
import org.json4s.native.JsonParser.parse

object JSONParsers {

  object StringToLong extends CustomSerializer[Long](format => ({ case JString(x) => x.toLong }, { case x: Long => JInt(x) }))

  implicit val formats = org.json4s.DefaultFormats + StringToLong

  def parseJson[T](inputJson: String)(implicit m: Manifest[T]):T = {
    parse(inputJson).extract[T]
  }
}
