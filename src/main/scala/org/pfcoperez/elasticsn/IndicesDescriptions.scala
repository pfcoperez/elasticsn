package org.pfcoperez.elasticsn

import com.sksamuel.elastic4s.analyzers.EnglishLanguageAnalyzer
import com.sksamuel.elastic4s.http.ElasticDsl._

object IndicesDescriptions {

  val mainIndex = "securitynow"
  val fineGrainedIndex = "securitynow_words"

  val createIndicesQueries = Seq(
    createIndex(mainIndex) mappings (
      mapping("episode") as (
        intField("number"),
        textField("title"),
        nestedField("text") fields (
          keywordField("speaker"),
          textField("line") analyzer EnglishLanguageAnalyzer
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
        textField("line") analyzer EnglishLanguageAnalyzer
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

}
