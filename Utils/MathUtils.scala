package org.rubigdata.Utils

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

object MathUtils {
   def calculateBayesianIndex(
        totalArticlesCol: Column, 
        densityCol: Column, 
        m: Double, 
        C: Double
    ): Column = {
        
        val v = totalArticlesCol
        val R = densityCol
        
        val mLit = lit(m)
        val CLit = lit(C)
        
        ((v / (v + mLit)) * R) + ((mLit / (v + mLit)) * CLit)
    }
}