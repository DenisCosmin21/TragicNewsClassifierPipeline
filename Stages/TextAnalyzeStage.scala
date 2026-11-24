package org.rubigdata.Stages

import org.rubigdata.Core.{PipelineStage, ParsedWarcRecord, AnalyzedRecord}
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.col

object TextAnalyzeStage {
    def mapRecord(record: ParsedWarcRecord, languageSpecificWords: Set[String]): AnalyzedRecord = {
        val allWords = record.words

        val wordCount = allWords.length

        val score = getScore(allWords, languageSpecificWords)

        val density = getDensity(wordCount, score)

        AnalyzedRecord(
            domain = record.domain,
            words = allWords,
            word_count = wordCount,
            tragedy_score = score,
            tragedy_density = density
        )
    }

    private def getScore(words: Array[String], languageSpecificWords: Set[String]): Int = {
        var score = 0

        if (languageSpecificWords.nonEmpty && words.nonEmpty) {
            for (word <- words) {
                if (languageSpecificWords.contains(word)) {
                    score += 1
                }
            }
        }

        score
    } 

    private def getDensity(wordCount: Int, score: Int): Double = {
        if (wordCount > 0) score.toDouble / wordCount.toDouble else 0.0
    }
}

class TextAnalyzeStage(spark : SparkSession, lexicon: Broadcast[Map[String, Set[String]]]) extends PipelineStage {
    override def process(df: DataFrame): DataFrame = {
        import spark.implicits._

        val textDs = df.as[ParsedWarcRecord];

        val localLexicon = lexicon;

        val analyzedDs = textDs.map(record => {
            val languageSpecificWords = localLexicon.value.getOrElse(record.language, Set.empty[String]);

            TextAnalyzeStage.mapRecord(record, languageSpecificWords)
        })

        analyzedDs.toDF
    }
}