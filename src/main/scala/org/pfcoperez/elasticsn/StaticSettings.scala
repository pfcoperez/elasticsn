package org.pfcoperez.elasticsn

import scala.concurrent.duration._

object StaticSettings {
  val transcriptsPath = "./transcripts"
  val httpUrl =  "elasticsearch://HOST:PORT?ssl=true"
  val bulkSize = 2000

  object Credentials {
    val user: String = "USER"
    val password: String = "PASS"
  }

  val maxDuration = 6 hours

}
