package org.pfcoperez.elasticsn.config

import scala.concurrent.duration.FiniteDuration

object ConfigEntities {

  case class Reader(transcriptsPath: String)

  case class Credentials(user: String, password: String)
  case class Connection(url: String, credentials: Credentials)
  case class Uploader(connection: Connection, bulkSize: Int, timeout: FiniteDuration)

  case class SecurityNow(reader: Reader, uploader: Uploader)

  case class SecurityNowConfig(securitynow: SecurityNow)

}
