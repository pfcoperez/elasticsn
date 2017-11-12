package org.pfcoperez.elasticsn

import scala.language.postfixOps
import com.sksamuel.elastic4s.http.NoOpRequestConfigCallback
import java.io.File


import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.ElasticDsl._

import pureconfig.loadConfig

import scala.concurrent.Future
import scala.util.Success
import scala.concurrent.ExecutionContext.Implicits.global

import Reader._
import IndicesDescriptions._
import interactions.IndexAndMappingsManipulation._
import interactions.IndexActions._


import config.ConfigEntities._

object Upload extends App with Logging {

  import com.typesafe.config.ConfigFactory

  //TODO: Don't crash without control when configuration is invalid
  val Right(config) = loadConfig[SecurityNowConfig](ConfigFactory.defaultReference())


  lazy val loadEpisodes: Future[List[Episode]] = Future.fromTry(
    loadDirectory(new File(config.securitynow.reader.transcriptsPath))
  )


  def uploadProcess(implicit client: HttpClient) = for {
    episodes <- loadEpisodes andThen {
      case Success(episodes) => log.info(s"Loaded ${episodes.size} episodes")
    }
    _ <- createIndices(createIndicesQueries) andThen {
      case Success(true) => log.info("Indices screated successfully")
      case Success(false) => log.error("Couldn't create some indices, trying to perform upload anyway...")
    }
    _ <- indexAllEntries(episodes, config.securitynow.uploader.bulkSize)
  } yield log.info("Indexing finished")

  implicit val client: HttpClient = {
    import config.securitynow.uploader.connection._

    val uri = ElasticsearchClientUri(s"elasticsearch://$url?ssl=true")

    val clientConfigCallback = new HttpClientConfigCallback {
      override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
        val credentialsProvider = {
          val provider = new BasicCredentialsProvider
          val userAndPass = new UsernamePasswordCredentials(credentials.user, credentials.password)
          provider.setCredentials(AuthScope.ANY, userAndPass)
          provider
        }
        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
      }
    }
    HttpClient(uri, NoOpRequestConfigCallback, clientConfigCallback)
  }

  uploadProcess.await(config.securitynow.uploader.timeout)

  client.close()

}
