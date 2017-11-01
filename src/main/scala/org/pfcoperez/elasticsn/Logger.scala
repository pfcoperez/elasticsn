package org.pfcoperez.elasticsn

import org.slf4j.LoggerFactory

trait Logging {
  protected lazy val log =
    LoggerFactory.getLogger(getClass)
}
