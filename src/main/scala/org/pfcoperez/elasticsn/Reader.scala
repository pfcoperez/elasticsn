package org.pfcoperez.elasticsn

import java.util.Date
import java.io.File

import scala.util.parsing.combinator._
import scala.util.{Try, Failure}
import scala.util.control.NonFatal
import scala.io.Source

import scala.language.postfixOps

object Reader extends Logging {

  case class Entry(speaker: String, line: String)

  case class Header(
    number: Int,
    title: String,
    date: Date,
    hosts: String,
    audio: String,
    site: String
  )

  case class Episode(header: Header, text: Seq[Entry])

  object Parser extends RegexParsers {

    def attribution: Parser[String] = "GIBSON RESEARCH CORPORATION" ~ "https://www.GRC.com/" ^^ {
      case owner ~ url => s"$owner $url"
    }

    def textLine: Parser[String] = """.+""".r ^^ { _.toString }

    def headerField(fieldName: String*): Parser[String] =
      s"""${fieldName.mkString("(", "|", ")")}:""".r ~ textLine ^^ {
        case field ~ value => value
      }

    def entry: Parser[Entry] =
      """.+:""".r ~ textLine ^^ {
        case speaker ~ line => Entry(speaker, line)
      }

    def header: Parser[Header] =
      headerField("SERIES") ~ headerField("EPISODE") ~ headerField("DATE") ~
        headerField("TITLE") ~ headerField("HOSTS", "SPEAKERS") ~ headerField("SOURCE", "SOURCE FILE") ~
        headerField("FILE", "FILE ARCHIVE", "ARCHIVE") ^^ {
          case _ ~ number ~ date ~ title ~ hosts ~ source ~ archieve =>
            Header(number.tail.toInt, title, new Date(date), hosts, source, archieve)
      }

    type ProtoEpisode = (Header, Seq[String])

    def protoEpisode: Parser[ProtoEpisode] =
      header ~ rep(textLine) ^^ {
        case header ~ lines => header -> lines
      }

    def episode: Parser[Episode] =
      attribution ~ header ~ rep(entry) ^^ {
        case _ ~ header ~ entries => Episode(header, entries)
      }

  }

  def readTranscript(file: File): Try[Episode] = Try {
    val contents = Source.fromFile(file).getLines.toSeq.tail.dropRight(3) map {
      _.trim
    } mkString("\n")
    val entry = """([A-Z]+):\s(.+)""".r
    val (header, lines) = Parser.parse(Parser.protoEpisode, contents).get

    val entries = (List.empty[Entry] /: lines) {
      case (acc, entry(k, v)) => Entry(k, v) :: acc
      case (Entry(lastSpeaker, lastLine)::prev, line) =>
        Entry(lastSpeaker, lastLine ++ line) :: prev
    }.reverse

    Episode(header, entries)
  } recoverWith {
    case NonFatal(e) =>
      log.error(s"Failed to parse episode: ${file.getPath}\n$e")
      Failure(e)
  }

  def loadDirectory(file: File, tolerateFailures: Boolean = true): Try[List[Episode]] = {
    Try {
      assert(file.isDirectory)
      val fs = file.listFiles
      assert(fs.nonEmpty)
      fs
    } flatMap { files =>

      import cats.syntax.traverse._
      import cats.instances.list._
      import cats.instances.try_._

      files.toList collect {
        case f if f.getPath.endsWith(".txt") && f.canRead() =>
          readTranscript(f)
      } filter(_.isSuccess || !tolerateFailures) sequence 

    }
  }

}

