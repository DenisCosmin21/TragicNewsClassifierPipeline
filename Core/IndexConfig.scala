package org.rubigdata.Core

import org.apache.spark.sql.DataFrame

case class IndexConfig(
    indexPath : String,
    identifiersPath : String,
    indexCheckpointPath : String,
);