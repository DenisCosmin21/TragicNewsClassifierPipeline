package org.rubigdata.Utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object DomainUtils {
    def getDomainTld(domainColumn: Column): Column = {
        element_at(split(domainColumn, "\\."), -1)
    }
}