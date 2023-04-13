package io.delta.core.internal

import scala.collection.mutable.ArrayBuffer

import io.delta.core.{Scan, ScanTask, Snapshot, Table}
import io.delta.core.data.{ColumnarBatch, ColumnVector}
import io.delta.core.expressions.{Expression, Literal}
import io.delta.core.helpers.{ConnectorReadContext, DefaultConnectorReadContext, DefaultTableHelper}
import io.delta.core.types.{IntegerType, StructType}
import io.delta.core.util.GoldenTableUtils
import io.delta.core.utils.CloseableIterator
import org.scalatest.funsuite.AnyFunSuite

class DefaultTableHelperSuite extends AnyFunSuite with GoldenTableUtils {
  Seq("column-mapping-name", "column-mapping-id").foreach(tableName =>
    test(s"end-to-end usage: reading a table with: $tableName") {
      withGoldenTable(tableName) { path =>
        val table = Table.forPath(path, new DefaultTableHelper())
        val snapshot = table.getLatestSnapshot

        // Contains both the data schema and partition schema
        val tableSchema = snapshot.getSchema;

        // Go through the tableSchema and select the columns interested in reading
        val readSchema = new StructType().add("value", IntegerType.INSTANCE)
        val filter = Literal.TRUE;

        val scanObject = scan(snapshot, readSchema, filter);

        val batches = toBatches(
          new DefaultConnectorReadContext(filter),
          readSchema,
          scanObject.getTasks())

        val actualValueColumnValues = ArrayBuffer[Int]()
        for(batch <- batches) {
          val valueColVector = batch.getColumnVector(0)
          actualValueColumnValues.append(vectorToInts(valueColVector): _*)
        }
        assert(actualValueColumnValues.toSet === Seq.range(start = 0, end = 20).toSet)
      }
    })

  test("end-to-end usage: reading a partitioned table") {
    withGoldenTable("partitioned") { path =>
      val table = Table.forPath(path, new DefaultTableHelper())
      val snapshot = table.getLatestSnapshot

      // Contains both the data schema and partition schema
      val tableSchema = snapshot.getSchema;

      {
        // Just select the regular column
        val readSchema = new StructType().add("id", IntegerType.INSTANCE)
        val filter = Literal.TRUE;

        val scanObject = scan(snapshot, readSchema, filter);

        val batches = toBatches(
          new DefaultConnectorReadContext(filter),
          readSchema,
          scanObject.getTasks())

        val actualIDColumnValues = ArrayBuffer[Int]()
        for (batch <- batches) {
          val idColVector = batch.getColumnVector(0)
          actualIDColumnValues.append(vectorToInts(idColVector): _*)
        }
        assert(actualIDColumnValues.toSet === Seq.range(start = 0, end = 2000).toSet)
      }

      {
        // Just select the partition column
        val readSchema = new StructType().add("partCol", IntegerType.INSTANCE)
        val filter = Literal.TRUE;

        val scanObject = scan(snapshot, readSchema, filter);

        val batches = toBatches(
          new DefaultConnectorReadContext(filter),
          readSchema,
          scanObject.getTasks())

        val actualPartColColumnValues = ArrayBuffer[Int]()
        for (batch <- batches) {
          val partColVector = batch.getColumnVector(0)
          actualPartColColumnValues.append(vectorToInts(partColVector): _*)
        }
        assert(actualPartColColumnValues.size === 2000)
        assert(actualPartColColumnValues.toSet === Seq.range(start = 0, end = 2000, step = 5).toSet)
      }
    }
  }

  private def toBatches(
      readContext: ConnectorReadContext,
      readSchema: StructType,
      taskIterator: CloseableIterator[ScanTask]): Seq[ColumnarBatch] = {
    val batches = new ArrayBuffer[ColumnarBatch]()
    try {
      while (taskIterator.hasNext) {
        val task = taskIterator.next()
        val data = task.getData(readContext, readSchema)
        while (data.hasNext) {
          batches.append(data.next())
        }
      }
    } finally {
      taskIterator.close()
    }
    batches.toSeq
  }

  private def scan(snapshot: Snapshot, readSchema: StructType, filter: Expression = null): Scan = {
    var builder = snapshot.getScanBuilder().withReadSchema(readSchema)
    if (filter != null) {
      builder = builder.withFilter(filter)
    }
    builder.build()
  }

  private def vectorToInts(intColumnVector: ColumnVector): Seq[Int] = {
    Seq.range(start = 0, end = intColumnVector.getSize)
      .map(rowId => intColumnVector.getInt(rowId))
      .toSeq
  }
}
