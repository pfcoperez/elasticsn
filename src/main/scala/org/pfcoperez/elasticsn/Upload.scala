package org.pfcoperez.elasticsn

import scala.language.postfixOps

import com.sksamuel.elastic4s.http.NoOpRequestConfigCallback
import io.circe.Json
import java.io.File
import java.util.Date
import org.apache.http.auth.{ AuthScope, UsernamePasswordCredentials }
import org.apache.http.impl.client.BasicCredentialsProvider

import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback

import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.ElasticDsl._
import io.circe.generic.auto._
import io.circe.Encoder
import com.sksamuel.elastic4s.circe._

import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import Reader._

object Upload extends App with Logging {

  object Settings {
    val transcriptsPath = "./transcripts"
    val httpUrl =  "elasticsearch://HOST:PORT?ssl=true"
    val bulkSize = 2000

    object Credentials {
      val user: String = "USER"
      val password: String = "PASSWORD"
    }

    val maxDuration = 6 hours

  }

  import Settings._

  implicit val dateEncoder: Encoder[Date] = new Encoder[Date] {
    def apply(d: Date): Json = {
      Json.fromLong(d.getTime)
    }
  }

  loadDirectory(new File(transcriptsPath)) foreach { episodes =>

    log.info(s"Loaded ${episodes.size} episodes")

    val uri = ElasticsearchClientUri(
      httpUrl
    )

    val index = "securitynow"
    val fineGrainedIndex = "securitynow_words"

    val createIndicesQueries = Seq(
      createIndex(index) mappings (
        mapping("episode") as (
          intField("number"),
          textField("title"),
          nestedField("text") fields (
            keywordField("speaker"),
            textField("line")
          ),
          dateField("date"),
          textField("speakers"),
          textField("audioURL"),
          textField("notesURL")
        ),
        mapping("episodeLine") as (
          intField("episodeNumber"),
          dateField("episodeDate"),
          keywordField("speaker"),
          textField("line")
        )
      ),
      createIndex(fineGrainedIndex) mappings (
        mapping("episodeWord") as (
          intField("episodeNumber"),
          dateField("episodeDate"),
          keywordField("speaker"),
          textField("line"),
          keywordField("word")
        )
      )
    )

    val insertQueries = episodes.toStream flatMap {
      case episode @ Episode(header, text) =>
        val episodeInsert = indexInto(index / "episode") doc episode
        val linesAndWordsToInsert = text.toStream flatMap {
          case Entry(speaker, line) =>
            import header._
            val lineIndexQuery = indexInto(index / "episodeLine") fields (
              "episodeNumber" -> number,
              "episodeDate" -> date,
              "speaker" -> speaker,
              "line" -> line
            )

            val charactersToKeep = (('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).toSet

            val wordIndexQueries = line.split(" ").toStream collect {
              case word if word.nonEmpty =>
                indexInto(fineGrainedIndex / "episodeWord" ) fields (
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

    //log.info(s"Generated ${insertQueries.size} queries")

    val insertBulks = insertQueries.grouped(bulkSize)

    val client = {

      import Credentials._

      val clientConfigCallback = new HttpClientConfigCallback {
        override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
          val credentialsProvider = {
            val provider = new BasicCredentialsProvider
            val credentials = new UsernamePasswordCredentials(user, password)
            provider.setCredentials(AuthScope.ANY, credentials)
            provider
          }
          httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
        }
      }
      HttpClient(uri, NoOpRequestConfigCallback, clientConfigCallback)
    }

    val asyncIndicesCreation: Future[Boolean] = Future.sequence {
      createIndicesQueries map { query =>
        client.execute(query).map(_.acknowledged) recoverWith {
          case NonFatal(e) =>
            log.error(s"Failed to create index: $query")
            Future.failed(e)
        }
      }
    } map (_.forall(identity))

    val asyncCreationAndIndexing: Future[Unit] = {
      val firstStep = asyncIndicesCreation collect {
        case true => log.info("Indices created")
      }
      /* Using foldLeft here instead of `Future.sequence`
         in order serialize bulks upload requests. That is, to avoid
         uploading two or more bulks in parallel. */
      (firstStep /: insertBulks.zipWithIndex) {
        case (prevStep, (queries, bulkNo)) =>
          for {
            _ <- prevStep
            bulkResult <- {
              log.info(s"Uploading bulk #$bulkNo")
              client.execute(bulk(queries:_*))
            }
          } yield {
            log.info(s"Done with bulk #$bulkNo with ${bulkResult.failures.size} failures")
          }
      }
    }

    asyncCreationAndIndexing.await(maxDuration)

    log.info("DONE UPLOADING")

    client.close()

  }

}
