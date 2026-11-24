package org.rubigdata.Core

case class AnalyzedRecord(
    domain: String,
    words: Array[String],
    word_count: Int,
    tragedy_score: Int,
    tragedy_density: Double
);