package org.pfcoperez.elasticsn.interactions

import com.sksamuel.elastic4s.http.ElasticDsl.{bulk, indexInto}
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.ElasticDsl._
import io.circe.Json
import java.util.Date

import io.circe.generic.auto._
import io.circe.Encoder
import com.sksamuel.elastic4s.circe._
import org.pfcoperez.elasticsn.{IndicesDescriptions, Logging}
import org.pfcoperez.elasticsn.Reader.{Entry, Episode}
import IndicesDescriptions._

import scala.concurrent.{ExecutionContext, Future}

object IndexActions extends Logging {

  object CustomEncoders {
    implicit val dateEncoder: Encoder[Date] = new Encoder[Date] {
      def apply(d: Date): Json = {
        Json.fromLong(d.getTime)
      }
    }
  }

  import CustomEncoders._

  def indexAllEntries(episodes: List[Episode], bulkSize: Int)(
    implicit client: HttpClient, ec: ExecutionContext): Future[Unit] = {

    val insertQueries = episodes.toStream flatMap {
      case episode@Episode(header, text) =>
        val episodeInsert = indexInto(mainIndex / "episode") doc episode
        val linesAndWordsToInsert = text.toStream flatMap {
          case Entry(speaker, line) =>
            import header._
            val lineIndexQuery = indexInto(mainIndex / "episodeLine") fields(
              "episodeNumber" -> number,
              "episodeDate" -> date,
              "speaker" -> speaker,
              "line" -> line
            )

            val charactersToKeep = (('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).toSet

            val wordIndexQueries = line.split(" ").toStream collect {
              case word if word.nonEmpty =>
                indexInto(fineGrainedIndex / "episodeWord") fields(
                  "episodeNumber" -> number,
                  "episodeDate" -> date,
                  "speaker" -> speaker,
                  "line" -> line,
                  "word" -> word.trim.filter(charactersToKeep)
                )
            }

            lineIndexQuery +: wordIndexQueries
        }
        episodeInsert +: linesAndWordsToInsert
    }

    val insertBulks = insertQueries.grouped(bulkSize)

    /* Using foldLeft here instead of `Future.sequence`
   in order serialize bulks upload requests. That is, to avoid
   uploading two or more bulks in parallel. */
    (Future.successful(()) /: insertBulks.zipWithIndex) {
      case (prevStep, (queries, bulkNo)) =>
        for {
          _ <- prevStep
          bulkResult <- {
            log.info(s"Uploading bulk #$bulkNo")
            client.execute(bulk(queries: _*))
          }
        } yield {
          log.info(s"Done with bulk #$bulkNo with ${bulkResult.failures.size} failures")
        }
    }

  }

}
