package org.rubigdata.OutputWritters

import org.apache.spark.sql.DataFrame
import org.rubigdata.Core.OutputWriter
import org.apache.spark.sql.SparkSession

class FileWriter(spark: SparkSession, path: String) extends OutputWriter {
    override def write(df: DataFrame): Unit = {
        
        spark.sparkContext.setJobGroup("file_storing_result", "Store results in a file");

        df.coalesce(1).write.mode("overwrite").parquet(path);
    }
}