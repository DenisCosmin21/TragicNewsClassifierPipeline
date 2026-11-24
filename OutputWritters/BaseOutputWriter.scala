package org.rubigdata.OutputWritters

import org.apache.spark.sql.DataFrame
import org.rubigdata.Core.OutputWriter
import org.apache.spark.sql.SparkSession

class BaseOutputWriter(spark: SparkSession, writers: OutputWriter*) extends OutputWriter {
    override def write(df: DataFrame): Unit = {
        df.cache();

        for (writer <- writers) {
            writer.write(df);
        }

        df.unpersist();
    }
}