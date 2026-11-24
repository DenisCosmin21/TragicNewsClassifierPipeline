package org.rubigdata.Utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.WrappedArray
import scala.collection.mutable
import org.apache.spark.sql.types._
import org.apache.spark.broadcast.Broadcast

object LanguageUtils {
    private var cachedLexiconMap: mutable.Map[String, Map[String, Set[String]]] = mutable.Map();

    private var cachedBroadcastMap: mutable.Map[String, Broadcast[Map[String, Set[String]]]] = mutable.Map()

    private def readLanguageMapping(spark: org.apache.spark.sql.SparkSession, languagePath: String): Map[String, Set[String]] = {
        val languageSchema = StructType(
            StructField("languages", MapType(StringType, ArrayType(StringType)), true) :: Nil
        )

        val languageDf = spark.read
            .option("multiline", "true")
            .schema(languageSchema)
            .json(languagePath)

        val row = languageDf.first()

        val lexiconMap = row.getAs[Map[String, Seq[String]]]("languages")

        lexiconMap.map { case (lang, keywords) => 
            lang -> keywords.toSet
        }
    }

    def getLanguageMapping(spark: org.apache.spark.sql.SparkSession, languagePath: String): Map[String, Set[String]] = {
        if (cachedLexiconMap.isEmpty) {
            spark.sparkContext.setJobGroup("load_languages", "Initialization: Loading Language Lexicon from HDFS")

            val lexiconMap = readLanguageMapping(spark, languagePath);

            cachedLexiconMap(languagePath) = lexiconMap;

            spark.sparkContext.setJobGroup(null, null)
        }

        cachedLexiconMap.get(languagePath).get;
    }

    def getSupportedLanguages(spark: SparkSession, languagePath: String): List[String] = {
        val languageMapping = getLanguageMapping(spark, languagePath)

        languageMapping.keySet.toList
    }

    def getBroadcastLexicon(spark: SparkSession, languagePath: String): Broadcast[Map[String, Set[String]]] = {
        if (!cachedBroadcastMap.contains(languagePath)) {
            spark.sparkContext.setJobGroup("broadcast_languages", "Initialization: Broadcasting Language Lexicon")
            
            val rawMap = getLanguageMapping(spark, languagePath)
            
            val broadcastMap = spark.sparkContext.broadcast(rawMap)
            cachedBroadcastMap(languagePath) = broadcastMap
            
            spark.sparkContext.setJobGroup(null, null)
        }
        cachedBroadcastMap(languagePath)
    }
}