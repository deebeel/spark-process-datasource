package org.deebeel.source

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{FunSpec, Matchers}

class DefaultSourceSpec extends FunSpec with Matchers{
  it("aa"){
    val spark = SparkSession.builder().master("local").getOrCreate()
    val c = spark.read
      .format("org.deebeel.source")
      .option("exec", "node")
      .schema(StructType(Seq(
        StructField("a", StringType)
      )))
      .load("/Users/smarkov/projects/spark_script_data_source/src/test/resources/tes.js")
    c.show()
  }
}
