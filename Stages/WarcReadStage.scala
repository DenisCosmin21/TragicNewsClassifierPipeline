package org.rubigdata.Stages

import org.rubigdata.Core.PipelineStage
import org.rubigdata.Core.IndexRecord
import org.rubigdata.Core.ParsedWarcRecord
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FSDataInputStream}
import scala.io.Source
import org.jwat.warc.{WarcReaderFactory, WarcRecord}
import org.jsoup.Jsoup
import scala.io.Codec
import scala.collection.JavaConverters._
import org.apache.spark.TaskContext
import org.apache.spark.sql.functions.col
import scala.util.matching.Regex
import org.apache.log4j.Logger
import scala.util.Try
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

object WarcReadStage {
    val lowerCharactersLimit = 100;
    val lowerWordsLimit = 30;

    @transient lazy val jsonMapper: ObjectMapper = new ObjectMapper()

    def extractHtmlContent(record: WarcRecord): Option[String] = {
        if (record.header.warcTypeStr == "response" && record.getPayload != null) {
            
            val httpHeader = record.getHttpHeader()
  
            if (
                httpHeader != null && 
                httpHeader.statusCode == 200 && 
                httpHeader.contentType != null && 
                httpHeader.contentType.contains("text/html")
            ) {
                val payloadStream = record.getPayload.getInputStream()

                
                val codec = Codec("UTF-8")

                codec.onMalformedInput(java.nio.charset.CodingErrorAction.REPLACE)
                codec.onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPLACE)
                
                val rawHtml = scala.io.Source.fromInputStream(payloadStream)(codec).mkString
              
                Some(rawHtml)
              
            } else {
                None 
            }
        } else {
            None
        }
    }

    def tryToGetFromScript(doc: org.jsoup.nodes.Document): Option[String] = {
        val jsonLdScripts = doc.select("script[type=application/ld+json]").asScala
        
        for (script <- jsonLdScripts) {
            println(">>> Trying to get from script")
            val jsonText = script.html()
            println(jsonText)
            if (jsonText.contains("\"Article\"") || jsonText.contains("\"NewsArticle\"")) { 
                
                // 2. Reuse the global parser for millions of records!
                val parsedJson: Try[JsonNode] = Try(jsonMapper.readTree(jsonText))
                
                if (parsedJson.isSuccess) {
                    val rootNode = parsedJson.get
                    val bodyNode = rootNode.path("articleBody")
                    
                    if (!bodyNode.isMissingNode && bodyNode.isTextual) {
                        val cleanText = bodyNode.asText()
                            .toLowerCase
                            .replaceAll("[^\\p{L}\\s]+", " ")
                            .replaceAll("\\s+", " ")
                            .trim
                            
                        if (cleanText.length >= lowerCharactersLimit) {
                            return Some(cleanText)
                        }
                    }
                }
            }
        }
        None
    }

def cleanHtmlToText(rawHtml: String): Option[String] = {
    try {
        val doc = Jsoup.parse(rawHtml)
        
        var extractedText: Option[String] = tryToGetFromScript(doc)

        if (extractedText.isEmpty) {
            println("Reading from article element")
            val articleElement = doc.select("article, [itemprop=articleBody], article-content, .post-content").first()

            if (articleElement != null) {
                val rawText = articleElement.select("p, h2, h3")
                    .asScala
                    .map(_.text())
                    .mkString("\n\n")
                    
                extractedText = Some(rawText) 
            }
        }

        extractedText match {
            case Some(text) =>
                val finalText = text
                    .toLowerCase
                    .replaceAll("[^\\p{L}\\s]+", " ")
                    .replaceAll("\\s+", " ")
                    .trim

                if (finalText.length >= lowerCharactersLimit) 
                    Some(finalText) 
                else 
                    None
                    
            case None => 
                None
        }

    } catch {
        case e: Exception =>
            println(e.getMessage())
            None
    }
}

    def tokenizeText(text: String): Array[String] = {
        text.split("\\s+")
    }
}

class WarcReadStage(spark: SparkSession, warcPath: String, filesOpenedCounter: org.apache.spark.util.LongAccumulator) extends PipelineStage {
    
    override def process(df: DataFrame): DataFrame = {
        
        import spark.implicits._

        val optimizedIndexDf = df
                .repartition(col("warc_filename"))
                .sortWithinPartitions(col("warc_filename"), col("warc_record_offset"));

        val indexDf = optimizedIndexDf.as[IndexRecord]

        val localWarcPath = warcPath;

        val localCounter = filesOpenedCounter

        val parsedHtmlDs = indexDf.mapPartitions(partition => {
            val conf = new Configuration()
            val fs = FileSystem.get(conf)
      
            var currentFileName = ""
            var hdfsStream: FSDataInputStream = null
      
            TaskContext.get().addTaskCompletionListener[Unit] { _ =>
                if (hdfsStream != null) {
                    hdfsStream.close()
                }
            }

            partition.flatMap(indexRecord => {
                if(currentFileName != indexRecord.warc_filename) {

                    if(hdfsStream != null)
                        hdfsStream.close()

                    hdfsStream = fs.open(new Path(localWarcPath + indexRecord.warc_filename))
                    
                    localCounter.add(1)

                    currentFileName = indexRecord.warc_filename
                }

                hdfsStream.seek(indexRecord.warc_record_offset)

                val warcReader = WarcReaderFactory.getReaderCompressed(hdfsStream)
                
                Option(warcReader.getNextRecord())
                    .flatMap(WarcReadStage.extractHtmlContent)
                    .flatMap(WarcReadStage.cleanHtmlToText)
                    .filter(_.length >= WarcReadStage.lowerCharactersLimit)
                    .map(WarcReadStage.tokenizeText)
                    .filter(_.length >= WarcReadStage.lowerWordsLimit)
                    .map(words => ParsedWarcRecord(
                        domain = indexRecord.url_host_registered_domain,
                        language = indexRecord.content_languages.split(",")(0),
                        words = words
                    ))
            })
        })
        
        parsedHtmlDs.toDF()
    }
}