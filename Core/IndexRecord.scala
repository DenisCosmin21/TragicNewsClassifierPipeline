package org.rubigdata.Core

case class IndexRecord(
    warc_filename: String,
    content_languages: String,
    warc_record_offset: Long,
    warc_record_length: Long,
    url_host_registered_domain: String
);