package org.rubigdata.Core

import org.apache.spark.sql.DataFrame

trait OutputGenerator {
    def generateOutput(df: DataFrame): Unit
}