package org.deebeel.source

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType



class DefaultSource extends SchemaRelationProvider {

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation =

    ScriptRelation(schema, parameters("exec"), Array(ScriptPartition(
      Seq(parameters("path")), 0
    )))(sqlContext)
}