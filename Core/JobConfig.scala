package org.rubigdata.Core
import scala.collection.mutable.Set

case class JobConfig(
    geographyConfig : GeographyConfig,
    indexConfig : IndexConfig,
    warcConfig : WarcConfig,
    analyzeConfig : AnalyzeConfig,
    outputConfig : OutputsConfig,
    stagesReset : Set[String],
    forceReset : Boolean
);