package org.rubigdata.Loaders

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI

object CheckpointManager {

    def getOrCreate(spark: SparkSession, checkpointPathStr: String, forceReset: Boolean, stageName: String = "")
                   (buildLogic: => DataFrame): DataFrame = {
        if (forceReset) {
            deleteCheckpoint(spark, checkpointPathStr)
            println(s">>> Rebuilding checkpoint...")
            executeAndSave(spark, buildLogic, checkpointPathStr, stageName)
        }
        else{
            loadCheckpoint(spark, checkpointPathStr, stageName).getOrElse {
                println(s">>> No checkpoint found. Executing build logic for: $checkpointPathStr")
                executeAndSave(spark, buildLogic, checkpointPathStr, stageName)
            }
        }
    }

    def generateHash(params: String*): String = {
        val md = java.security.MessageDigest.getInstance("MD5")
        md.digest(params.mkString("-").getBytes).map("%02x".format(_)).mkString
    }

    private def loadCheckpoint(spark: SparkSession, pathStr: String, stageName: String = ""): Option[DataFrame] = {
        val fs = FileSystem.get(new URI("hdfs:///"), spark.sparkContext.hadoopConfiguration)
        val path = new Path(pathStr)
        val successPath = new Path(path + "/_SUCCESS")
        
        if (fs.exists(path)) {
            println(s">>> Success: Existing checkpoint found for stage $stageName! Loading instantly...")
            spark.sparkContext.setJobGroup(s"loading_$stageName", s"Loading checkpoint for $stageName")
            Some(spark.read.parquet(pathStr))
        } else {
            None 
        }
    }

    private def deleteCheckpoint(spark: SparkSession, pathStr: String): Unit = {
        val fs = FileSystem.get(new URI("hdfs:///"), spark.sparkContext.hadoopConfiguration)
        val path = new Path(pathStr)

        if (fs.exists(path)) {
            println(s">>> Force reset. Deleting existing checkpoint at: $pathStr")
            fs.delete(path, true)
        }
    }

    private def executeAndSave(spark: SparkSession, buildLogic: => DataFrame, pathStr: String, stageName: String = ""): DataFrame = {
        spark.sparkContext.setJobGroup(s"reading_$stageName", s"Reading data from HDFS for $stageName")
        println(s">>> Reading data from HDFS for: $stageName")

        val processedDf = buildLogic 
        saveCheckpoint(processedDf, pathStr)

        println(s">>> Success: Checkpoint saved to: $pathStr for $stageName")
        
        spark.read.parquet(pathStr)
    }

    private def saveCheckpoint(df: DataFrame, path: String): Unit = {
        df.write.mode("overwrite").parquet(path)
    }
}