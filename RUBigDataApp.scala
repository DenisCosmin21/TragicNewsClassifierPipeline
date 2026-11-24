package org.rubigdata

import org.apache.spark.sql.SparkSession
import org.rubigdata.Stages.IndexReadStage
import org.rubigdata.Core.{JobConfig, GeographyConfig, IndexConfig, WarcConfig, OutputsConfig, AnalyzeConfig, PipelineStage, DataBaseConfig}
import scala.collection.mutable.Set
import org.apache.spark.SparkConf
import org.rubigdata.Stages.WarcReadStage

object RUBigDataApp {
  def main(args: Array[String]) {
    
    val kryoConf = new SparkConf()
        .registerKryoClasses(Array(
            classOf[org.rubigdata.Core.IndexRecord],
            classOf[org.rubigdata.Core.ParsedWarcRecord],
            classOf[org.rubigdata.Core.AnalyzedRecord],
            classOf[Array[String]],
            classOf[Map[String, Set[String]]],
            classOf[Map[String, String]]
    ))

    val spark = SparkSession.builder.config(kryoConf)
    .appName("Tragedy News Checker")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryoserializer.buffer.max", "512m")
    .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val geographyConfig = GeographyConfig(
        languagePath = "hdfs:///user/s1181537/multilingual_tragedy_lexicon.json",
        countryMappingPath = "hdfs:///user/s1181537/flat_country_mappings.json"
    )

    val indexConfig = IndexConfig(
       //indexPath = "hdfs:///cc-index-subset/subset=warc/part-00299-74237c22-0523-49c6-9e5a-6b4aa471a042.c000.gz.parquet",
        // = "hdfs:///cc-index-subset/subset=warc/part-00251-74237c22-0523-49c6-9e5a-6b4aa471a042.c000.gz.parquet",
        indexPath = "hdfs:///cc-index-subset/subset=warc",
        identifiersPath = "hdfs:///user/s1181537/identifiers.txt",
        indexCheckpointPath = "hdfs:///user/s1181537/checkpoints/index_read_stage"
    );

    val warcConfig = WarcConfig(
        warcPath = "hdfs:///cc-crawl/",
        warcCheckpointPath = "hdfs:///user/s1181537/checkpoints/warc_read_stage",
    )

    val analyzeConfig = AnalyzeConfig(
        textAnalyzeCheckpointPath = "hdfs:///user/s1181537/checkpoints/text_analyze_stage",
    )

    val domainDbConfig = DataBaseConfig(
        url = "jdbc:postgresql://ep-shiny-water-a2s2kng7.aws-eu-central-1.pg.laravel.cloud:5432/main",
        user = "",
        password = "",
        driver = "org.postgresql.Driver",
        main_table = "domain_scores",
        staging_table = "staging_table"
    )

    val outputConfig = OutputsConfig(
        domainOutputPath = "hdfs:///user/s1181537/results/domain_outputs",
        domainDataBaseConfig = domainDbConfig,
        countryOutputPath = "hdfs:///user/s1181537/results/country_outputs",
    )

    val config = JobConfig(
        geographyConfig = geographyConfig,
        indexConfig = indexConfig,
        warcConfig = warcConfig,
        analyzeConfig = analyzeConfig,
        outputConfig = outputConfig,
        forceReset = args.contains("--reset-all"),
        stagesReset = getResetStages(args)
    );

    val pipelineOrchestror = new PipelineOrchestror(spark, config)

    pipelineOrchestror.runPipeline()

    spark.stop()
  }

  private def getResetStages(args: Array[String]): Set[String] = {
    val resetStages = Set[String]()

    args.foreach {
            case "--index"     => resetStages += "IndexReadStage"
            case "--warc"      => resetStages += "WarcReadStage"
            case "--nlp"       => resetStages += "TextAnalyzeStage"
            case "--output"    => resetStages += "Output_Aggregation" 
            case _             =>
        }

    resetStages
  }
}
