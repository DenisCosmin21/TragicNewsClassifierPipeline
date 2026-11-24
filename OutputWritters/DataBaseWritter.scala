package org.rubigdata.OutputWritters

import org.rubigdata.Core.OutputWriter
import org.apache.spark.sql.SparkSession
import org.rubigdata.Core.DataBaseConfig
import org.apache.spark.sql.{DataFrame, SaveMode}
import java.util.Properties
import java.sql.DriverManager
import org.apache.spark.sql.functions.col

class DataBaseWritter(spark: SparkSession, config: DataBaseConfig) extends OutputWriter
{
    override def write(df: DataFrame): Unit = {

        val dbUrl = config.url

        val connectionProperties = new Properties()

        Class.forName(config.driver)

        connectionProperties.put("user", config.user)
        connectionProperties.put("password", config.password)
        connectionProperties.put("driver", config.driver)
        connectionProperties.put("batchsize", "10000")

        val mainTable = config.main_table
        val stagingTable = config.staging_table

        val createStagingSql = s"""
            CREATE TABLE IF NOT EXISTS $stagingTable (
                domain_name VARCHAR(64),
                score DOUBLE PRECISION
            );
            TRUNCATE TABLE $stagingTable;
        """

        println(">>> Step 1: Writing data to Staging Table...")

        var connection: java.sql.Connection = null
        var statement: java.sql.Statement = null

        try {
            
            connection = DriverManager.getConnection(dbUrl, config.user, config.password)
            statement = connection.createStatement()
            statement.executeUpdate(createStagingSql)

            df.coalesce(1)
                .select(
                    col("domain").alias("domain_name"),
                    col("tragedy_score").alias("score")
                )
                .write
                .mode(SaveMode.Overwrite) 
                .jdbc(dbUrl, stagingTable, connectionProperties)
                
            println(">>> Step 2: Executing PostgreSQL Upsert...")

            

            val upsertQuery = s"""
                INSERT INTO $mainTable (domain_name, score)
                SELECT domain_name, score
                FROM $stagingTable
                ON CONFLICT (domain_name) 
                DO UPDATE SET 
                    score = EXCLUDED.score
            """
            
            statement.executeUpdate(upsertQuery)
            
            statement.executeUpdate(s"DROP TABLE $stagingTable;")
            
            println(">>> Upsert complete and staging table dropped!")
            
        } catch {
            case e: Exception => 
                System.err.println(s"Database Upsert Failed: ${e.getMessage}")
                throw e
        } finally {
            if (statement != null) statement.close()
            if (connection != null) connection.close()
        }
    }
}