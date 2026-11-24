package org.rubigdata.OutputGenerators

import org.apache.spark.sql.DataFrame
import org.rubigdata.Core.{OutputWriter, OutputGenerator}
import org.rubigdata.Utils.MathUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object DomainOutputGenerator {
    def getGlobalAverage(df: DataFrame): Double = {
        val record = df.select(avg("average_tragedy_density")).first();

        record.getDouble(0)
    }
}

class DomainOutputGenerator(spark: SparkSession, writer: OutputWriter) extends OutputGenerator {
    override def generateOutput(df: DataFrame): Unit = {
        println("Generating output for domains");

        spark.sparkContext.setJobGroup("generating_domain_report", "Generating Report for domains");

        val minimumThreshold = 30;

        val domainReport = df
            .groupBy("domain")
            .agg(
                count("*").alias("total_articles"),
                sum("word_count").alias("total_words_analyzed"),
                sum("tragedy_score").alias("absolute_tragedy_score"),
                avg("tragedy_density").alias("average_tragedy_density")
            ).where("total_articles > 30");


        val globalAverage = DomainOutputGenerator.getGlobalAverage(domainReport);

        val finalReport = domainReport
            .withColumn("bayesian_index", 
                MathUtils.calculateBayesianIndex(
                    col("total_articles"), 
                    col("average_tragedy_density"), 
                    minimumThreshold, 
                    globalAverage
                ) * 10000
            ).drop("total_articles", "total_words_analyzed", "absolute_tragedy_score", "average_tragedy_density");
        
        val windowSpec = Window.orderBy("bayesian_index")

        val finalScoredReport = finalReport.withColumn(
            "tragedy_score",
            round(percent_rank().over(windowSpec) * 100, 2) 
        )

        writer.write(finalScoredReport);
    }
}