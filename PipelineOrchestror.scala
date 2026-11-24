package org.rubigdata 

import org.apache.spark.sql.SparkSession
import org.rubigdata.Core.JobConfig
import org.rubigdata.Loaders.CheckpointManager
import org.rubigdata.Stages.{IndexReadStage, WarcReadStage, TextAnalyzeStage}
import org.rubigdata.Utils.{LanguageUtils, GeographyMappingUtils}
import org.rubigdata.OutputGenerators.{DomainOutputGenerator, CountryOutputGenerator}
import org.rubigdata.Factories.OutputWriterFactory
import org.apache.spark.sql.functions.col

class PipelineOrchestror(spark: SparkSession, config: JobConfig) {
    def runPipeline(): Unit = {
        
        val filesOpenedCounter = spark.sparkContext.longAccumulator("Total WARC Files Opened")

        println(s">>> Starting pipeline with config: $config")

        val supportedLanguages = LanguageUtils.getSupportedLanguages(spark, config.geographyConfig.languagePath)
        
        val supportedCountries = GeographyMappingUtils.getSupportedCountries(spark, config.geographyConfig.countryMappingPath)

        val indexCheckpointIdentifier = CheckpointManager.generateHash("v1", config.indexConfig.indexPath, config.indexConfig.identifiersPath, supportedLanguages.sorted.mkString(","), supportedCountries.sorted.mkString(","));

        val indexCheckpointPath = s"${config.indexConfig.indexCheckpointPath}/checkpoint_$indexCheckpointIdentifier";

        var currentStage = "IndexReadStage";

        val filteredIndexDf = CheckpointManager.getOrCreate(spark, indexCheckpointPath, shouldReset(currentStage), currentStage) {
            val rawDf = spark.read.parquet(config.indexConfig.indexPath);

            val indexReadStage = new IndexReadStage(spark, config.indexConfig.identifiersPath, supportedLanguages, supportedCountries);

            indexReadStage.process(rawDf)
        } 

        /*spark.sparkContext.setJobGroup("printing_debug", "Debugging: Printing first 10 rows of filtered index");
        filteredIndexDf.show(10, false);*/
        currentStage = "WarcReadStage";

        val warcCheckpointIdentifier = CheckpointManager.generateHash(indexCheckpointIdentifier, config.warcConfig.warcPath);

        val warcCheckpointPath = s"${config.warcConfig.warcCheckpointPath}/checkpoint_$warcCheckpointIdentifier";
        
        val parsedWarcDf = CheckpointManager.getOrCreate(spark, warcCheckpointPath, shouldReset(currentStage), currentStage) {
            val warcReadStage = new WarcReadStage(spark, config.warcConfig.warcPath, filesOpenedCounter);

            warcReadStage.process(filteredIndexDf)
        }

        spark.sparkContext.setJobGroup("printing_debug", "Debugging: Printing first 10 rows of parsed warc");
        parsedWarcDf.show(10, false);

        val broadcastLexicon = LanguageUtils.getBroadcastLexicon(spark, config.geographyConfig.languagePath);

        val analyzedCheckpointIdentifier = CheckpointManager.generateHash(warcCheckpointIdentifier, config.analyzeConfig.textAnalyzeCheckpointPath);

        val analyzedCheckpointPath = s"${config.analyzeConfig.textAnalyzeCheckpointPath}/checkpoint_$analyzedCheckpointIdentifier";

        currentStage = "TextAnalyzeStage";

        val analyzedDf = CheckpointManager.getOrCreate(spark, analyzedCheckpointPath, shouldReset(currentStage), currentStage) {
            val textAnalyzeStage = new TextAnalyzeStage(spark, broadcastLexicon);

            textAnalyzeStage.process(parsedWarcDf)
        }

        spark.sparkContext.setJobGroup("printing_debug", "Debugging: Printing first 10 rows of analyzed text");
        analyzedDf.show(10, false);
        
        println(s">>> Pipeline completed!")

        println(s">>> Generating outputs!")

        val domainWritter = OutputWriterFactory.make(spark, "domain", config);
        val countryWritter = OutputWriterFactory.make(spark, "country", config);

        val domainOutputGenerator = new DomainOutputGenerator(spark, domainWritter);

        val broadcastCountryMap = GeographyMappingUtils.getBroadcastCountryMap(spark, config.geographyConfig.countryMappingPath);

        val countryOutputGenerator = new CountryOutputGenerator(spark, countryWritter, broadcastCountryMap);

        domainOutputGenerator.generateOutput(analyzedDf);

        countryOutputGenerator.generateOutput(analyzedDf);

        println(s">>> Output generation completed!")
    }

    private def shouldReset(stage: String): Boolean = {
        config.forceReset || config.stagesReset.contains(stage)
    }
}