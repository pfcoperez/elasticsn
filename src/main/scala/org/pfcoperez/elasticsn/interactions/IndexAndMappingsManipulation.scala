package org.pfcoperez.elasticsn.interactions

import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.indexes.CreateIndexDefinition
import com.sksamuel.elastic4s.http.ElasticDsl._
import org.pfcoperez.elasticsn.Logging

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object IndexAndMappingsManipulation extends Logging {


  def createIndices(createIndicesQueries: Seq[CreateIndexDefinition])(
    implicit client: HttpClient, ec: ExecutionContext
  ): Future[Boolean] = Future.sequence {
    createIndicesQueries map { query =>
      client.execute(query).map(_.acknowledged) recoverWith {
        case NonFatal(e) =>
          log.error(s"Failed to create index: ${query.name}\n$e")
          //Future.failed(e)
          Future.successful(false)
      }
    }
  } map (_.forall(identity))

}
