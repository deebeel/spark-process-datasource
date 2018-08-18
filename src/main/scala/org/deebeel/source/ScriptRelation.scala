package org.deebeel.source

import java.io.{BufferedReader, InputStreamReader}

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType

case class ScriptIterator(schema: StructType, br: BufferedReader) extends Iterator[Row] {
  val enconder = RowEncoder(schema)
  @transient val row = new SpecificInternalRow(schema.fields.map(x => x.dataType))

  override def hasNext: Boolean = {
    val item = br.readLine()
    if (item == null) {
      br.close()
      false
    } else if(item.isEmpty){
      hasNext
    } else {
      item.split(",").zipWithIndex.foreach {
        case (v, i)=>row.update(i, v)
      }
      true
    }
  }

  override def next(): Row = {
    Row.fromSeq(row.values.map(_.boxed).toSeq)
  }
}

case class ScriptPartition(args: Seq[String], index: Int) extends Partition

case class ScriptRDD(schema: StructType, exec: String, parts: Array[ScriptPartition])(sc: SparkContext) extends RDD[Row](sc, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val (_, br) = split match {
      case ScriptPartition(args, _) =>
        val pb = new ProcessBuilder(Seq(exec) ++ args: _*)
        val p = pb.start()
        (new BufferedReader(new InputStreamReader(p.getErrorStream)), new BufferedReader(new InputStreamReader(p.getInputStream)))

    }
    new InterruptibleIterator(context, ScriptIterator(schema, br))
  }


  override protected def getPartitions: Array[Partition] = parts.asInstanceOf[Array[Partition]]
}

case class ScriptRelation(schema: StructType, exec: String, partitions: Array[ScriptPartition])(context: SQLContext)
  extends BaseRelation
    with TableScan {
  override def sqlContext: SQLContext = context

  override def buildScan(): RDD[Row] = {
    ScriptRDD(schema, exec, partitions)(context.sparkContext).asInstanceOf[RDD[Row]]
  }
}
