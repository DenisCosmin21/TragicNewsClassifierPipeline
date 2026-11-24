package org.rubigdata.Core

case class ParsedWarcRecord(
    domain: String,
    language: String,
    words: Array[String],
);