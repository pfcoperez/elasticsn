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
import scala.util.{Success, Failure}
import scala.concurrent.ExecutionContext.Implicits.global

import Reader._

object Upload extends App with Logging {

  import StaticSettings._

  implicit val dateEncoder: Encoder[Date] = new Encoder[Date] {
    def apply(d: Date): Json = {
      Json.fromLong(d.getTime)
    }
  }

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

  lazy val loadEpisodes: Future[List[Episode]] = Future.fromTry(loadDirectory(new File(transcriptsPath)))

  lazy val createIndices: Future[Boolean] = Future.sequence {
    createIndicesQueries map { query =>
      client.execute(query).map(_.acknowledged) recoverWith {
        case NonFatal(e) =>
          log.error(s"Failed to create index: ${query.name}")
          //Future.failed(e)
          Future.successful(false)
      }
    }
  } map (_.forall(identity))

  def indexAllEntries(episodes: List[Episode]): Future[Unit] = {

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
            client.execute(bulk(queries:_*))
          }
        } yield {
          log.info(s"Done with bulk #$bulkNo with ${bulkResult.failures.size} failures")
        }
    }

  }

  lazy val uploadProcess = for {
    episodes <- loadEpisodes andThen {
      case Success(episodes) => log.info(s"Loaded ${episodes.size} episodes")
    }
    _ <- createIndices andThen {
      case Success(true) => log.info("Indices screated successfully")
      case Success(false) => log.error("Couldn't create some indices, trying to perform upload anyway...")
    }
    _ <- indexAllEntries(episodes)
  } yield log.info("Indexing finished")

  val client = {

    val uri = ElasticsearchClientUri(httpUrl)

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

  uploadProcess.await(maxDuration)

  client.close()

}
