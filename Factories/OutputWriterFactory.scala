package org.rubigdata.Factories

import org.apache.spark.sql.DataFrame
import org.rubigdata.Core.{OutputsConfig, JobConfig, OutputWriter}
import org.rubigdata.OutputWritters.{BaseOutputWriter, FileWriter, DataBaseWritter}
import org.apache.spark.sql.SparkSession

object OutputWriterFactory {
    def make(spark: SparkSession, outputType: String, jobConfig: JobConfig): OutputWriter = {
        outputType match {
            case "domain" => getWriterForDomain(spark, jobConfig.outputConfig)
            case "country" => getWriterForCountry(spark, jobConfig.outputConfig)
            case _ => throw new IllegalArgumentException("Unknown output type: " + outputType)
        }
    }

    private def getWriterForDomain(spark: SparkSession, config: OutputsConfig): OutputWriter = {
        return new BaseOutputWriter(spark, new FileWriter(spark, config.domainOutputPath), new DataBaseWritter(spark, config.domainDataBaseConfig));
    }

    private def getWriterForCountry(spark: SparkSession, config: OutputsConfig): OutputWriter = {
        return new BaseOutputWriter(spark, new FileWriter(spark, config.countryOutputPath));
    }
}