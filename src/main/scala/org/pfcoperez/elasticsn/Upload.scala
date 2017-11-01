package org.pfcoperez.elasticsn

import scala.language.postfixOps

import com.sksamuel.elastic4s.http.NoOpRequestConfigCallback
import com.sksamuel.elastic4s.indexes.IndexDefinition
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
    val bulkSize = 500

    object Credentials {
      val user: String = "USER"
      val password: String = "PASSWORD"
    }

    val maxQueryTime = 4 hours

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
      )
    )

    val insertQueries: Seq[IndexDefinition] = episodes flatMap {
      case episode @ Episode(header, text) =>
        val episodeInsert = indexInto(index / "episode") doc episode
        val linesInsert = text map {
          case Entry(speaker, line) =>
            import header._
            indexInto(index / "episodeLine") fields (
              "episodeNumber" -> number,
              "episodeDate" -> date,
              "speaker" -> speaker,
              "line" -> line
            )
        }
        episodeInsert +: linesInsert
    }

    val insertBulks = insertQueries.grouped(bulkSize).toList

    val client ={

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


    if(asyncIndicesCreation.await(maxQueryTime)) {
      log.info(s"About to index ${insertQueries.size} entries in ${insertBulks.size} bulks of size $bulkSize")

      insertBulks.zipWithIndex foreach { case (queries, bulkNo) =>
        log.info(s"Uploading bulk #$bulkNo")

        val uploadFuture = {
          client.execute(bulk(queries:_*))
        }

        val res = uploadFuture.await(maxQueryTime)
        log.info(s"Done with bulk #$bulkNo with ${res.failures.size} failures")
      }

      log.info("Bulk upload finished")

    } else log.error("Couln't create index")

    log.info("DONE UPLOADING")

    client.close()

  }

}
