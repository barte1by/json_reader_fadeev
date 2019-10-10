
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.io.Source

object Stack extends App {
    implicit val formats = DefaultFormats // Brings in default date formats etc.

    case class BookDetails(id: Int, country: String, points: Int, title: String, variety: String, winery: String)

    for (line <- Source.fromFile("/home/fadeev/Desktop/winemag-data-130k-v2.json").getLines) {
      val bookDetails = jsonStrToMap(line)
      println(bookDetails)
    }

    def jsonStrToMap(jsonStr: String): BookDetails = {
      parse(jsonStr).camelizeKeys.extract[BookDetails]
    }
}
