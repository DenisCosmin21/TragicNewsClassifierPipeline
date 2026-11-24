package org.rubigdata.Core

import org.apache.spark.sql.DataFrame

trait OutputWriter {
    def write(df: DataFrame): Unit
}