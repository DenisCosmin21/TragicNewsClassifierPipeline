package org.rubigdata.Stages

import org.rubigdata.Core.PipelineStage
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.broadcast.Broadcast

object IndexReadStage{
    var broadcastNewsKeywords: Broadcast[Set[String]] = _

    def initBroadcast(spark: SparkSession, dictionaryHdfsPath: String): Unit = {
        if (broadcastNewsKeywords == null) {
            println(s"Loading Multi-Lingual News Dictionary from $dictionaryHdfsPath...")
            
            val keywordArray = spark.sparkContext.textFile(dictionaryHdfsPath).collect()
            
            val keywordSet = keywordArray.map(_.trim.toLowerCase).filter(_.nonEmpty).toSet
            
            println(s"Successfully loaded ${keywordSet.size} global news keywords.")
            
            broadcastNewsKeywords = spark.sparkContext.broadcast(keywordSet)
        }
    }
}

class IndexReadStage(spark: SparkSession, newsIdentifierPath: String, supportedLanguages: List[String], supportedCountries: List[String]) extends PipelineStage {
    override def process(df: DataFrame): DataFrame = {

        IndexReadStage.initBroadcast(df.sparkSession, newsIdentifierPath);
        
        val localBroadcast = IndexReadStage.broadcastNewsKeywords

        val isNewsUrl = udf((url: String) => {
            if (url == null) false
            else {
                val urlTokens = url.toLowerCase.split("[^a-z0-9]+")
                    
                urlTokens.exists(token => localBroadcast.value.contains(token))
            }
        })

        val patternToRemove = "^crawl-data/CC-MAIN-2021-17/";

        val langRegex = supportedLanguages.mkString("(", "|", ")")

        val countryRegex = supportedCountries.mkString("\\.(", "|", ")$")

        val filteredDf = df.select(
            "url",
            "fetch_status",
            "content_mime_type",
            "warc_filename",
            "warc_record_offset",
            "warc_record_length",
            "url_host_registered_domain",
            "content_languages"
        ).filter(
            col("fetch_status") === "200" 
            && col("content_mime_type") === "text/html"
            && col("content_languages").isNotNull
            && col("content_languages").rlike(langRegex)
            && isNewsUrl(col("url"))
            && col("url").startsWith("https://")
            && col("url_host_registered_domain").rlike(countryRegex)
        )
        .drop("fetch_status", "content_mime_type", "url")
        
        val mappedDf = filteredDf.withColumn("warc_filename", 
            regexp_replace(col("warc_filename"), patternToRemove, "")
        );

        mappedDf
    }
}