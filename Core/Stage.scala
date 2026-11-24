package org.rubigdata.Core

import org.apache.spark.sql.DataFrame

trait PipelineStage {
  def process(df: DataFrame): DataFrame
}