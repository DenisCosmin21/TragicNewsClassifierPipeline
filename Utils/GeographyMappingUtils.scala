package org.rubigdata.Utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import org.apache.spark.sql.types._
import org.apache.spark.broadcast.Broadcast

object GeographyMappingUtils {
    private var cachedCountriesMapping: mutable.Map[String, Map[String, String]] = mutable.Map();

    private var cachedBroadcastMap: mutable.Map[String, Broadcast[Map[String, String]]] = mutable.Map()

    def getCountryMapping(spark: SparkSession, countryPath: String): Map[String, String] = {
        if (cachedCountriesMapping.isEmpty) {
            spark.sparkContext.setJobGroup("load_countries", "Initialization: Loading Country Lexicon from HDFS")

            val countriesMapping = readCountryMapping(spark, countryPath);

            cachedCountriesMapping(countryPath) = countriesMapping;
        }
        cachedCountriesMapping(countryPath)
    }

    def getSupportedCountries(spark: SparkSession, countryPath: String): List[String] = {
        val countriesMapping = getCountryMapping(spark, countryPath)

        countriesMapping.keySet.toList
    }

    private def readCountryMapping(spark: SparkSession, countryPath: String): Map[String, String] = {
        val countrySchema = StructType(
            StructField("country_mappings", MapType(StringType, StringType), true) :: Nil
        );

        val countryDf = spark.read
            .option("multiline", "true")
            .schema(countrySchema)
            .json(countryPath)

        val row = countryDf.first() 

        row.getAs[Map[String, String]]("country_mappings")
    }

    def getBroadcastCountryMap(spark: SparkSession, countryPath: String): Broadcast[Map[String, String]] = {
        if (!cachedBroadcastMap.contains(countryPath)) {
            spark.sparkContext.setJobGroup("broadcast_languages", "Initialization: Broadcasting Countries map")
            
            val rawMap = getCountryMapping(spark, countryPath)
            
            val broadcastMap = spark.sparkContext.broadcast(rawMap)
                cachedBroadcastMap(countryPath) = broadcastMap
            
            spark.sparkContext.setJobGroup(null, null)
        }

        cachedBroadcastMap(countryPath)
    }
}