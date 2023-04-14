package io.delta.core

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import io.delta.core.data.{ColumnarBatch, ColumnVector, JsonRow, Row}
import io.delta.core.expressions.{Expression, Literal}
import io.delta.core.helpers.{ConnectorReadContext, DefaultConnectorReadContext, DefaultTableHelper}
import io.delta.core.types._
import io.delta.core.util.GoldenTableUtils
import io.delta.core.utils.CloseableIterator
import java.util
import org.scalatest.funsuite.AnyFunSuite

class DeltaCoreAPISuite extends AnyFunSuite with GoldenTableUtils {
  test("end-to-end usage: reading a table") {
    withGoldenTable("dv-table") { path =>
      val table = Table.forPath(path, new DefaultTableHelper())
      val snapshot = table.getLatestSnapshot

      // Contains both the data schema and partition schema
      val tableSchema = snapshot.getSchema

      // Go through the tableSchema and select the columns interested in reading
      val readSchema = new StructType().add("value", IntegerType.INSTANCE)
      val filter = Literal.TRUE

      val scanObject = scan(snapshot, readSchema, filter)

      val fileIter = scanObject.survivedFileInfo()
      val scanState = scanObject.getScanState();

      // There should be just one element in the scan state
      val scanStateColumnarBatch = scanState.next()
      val serializedScanState = convertColumnarBatchRowToJSON(scanStateColumnarBatch, 0)

      val connectorReadContext = new ConnectorReadContext {
        // connectors own implementation of read context
      }

      while(fileIter.hasNext) {
        val fileColumnarBatch = fileIter.next()
        Seq(0, fileColumnarBatch.getSize).foreach(rowId => {
          val serializedFileInfo = convertColumnarBatchRowToJSON(fileColumnarBatch, rowId)

          // START OF THE CODE THAT WILL BE EXECUTED ON THE EXECUTOR
          snapshot.getScanState.getData(
            convertJSONToRow(serializedFileInfo, fileColumnarBatch.getSchema),
            convertJSONToRow(serializedScanState, scanStateColumnarBatch.getSchema),
            readSchema,
            connectorReadContext
          )
          // END OF THE CODE THAT WILL BE EXECUTED ON THE EXECUTOR
        })
      }

      val batches = toBatches(
        new DefaultConnectorReadContext(filter),
        readSchema,
        scanObject.getTasks())

      val actualValueColumnValues = ArrayBuffer[Int]()
      for (batch <- batches) {
        val valueColVector = batch.getColumnVector(0)
        actualValueColumnValues.append(vectorToInts(valueColVector): _*)
      }
      assert(actualValueColumnValues.toSet === Seq.range(start = 0, end = 20).toSet)
    }
  }

  private def convertColumnarBatchRowToJSON(columnarBatch: ColumnarBatch, rowId: Int): String = {
    val rowObject = new java.util.HashMap[String, Object]()

    import scala.collection.JavaConverters._
    val schema = columnarBatch.getSchema
    schema.fields().asScala.zipWithIndex.foreach {
      case (field, index) =>
        val dataType = field.getDataType
        if (dataType.isInstanceOf[StructType]) {
          rowObject.put(field.getName, columnarBatch.getColumnVector(index).getStruct(0))
        } else if (dataType.isInstanceOf[ArrayType]) {
          rowObject.put(field.getName, columnarBatch.getColumnVector(index).getArray(0))
        } else if (dataType.isInstanceOf[MapType]) {
          rowObject.put(field.getName, columnarBatch.getColumnVector(index).getMap(0))
        } else if (dataType.isInstanceOf[IntegerType]) {
          rowObject.put(field.getName, new Integer(columnarBatch.getColumnVector(index).getInt(0)))
        } else if (dataType.isInstanceOf[LongType]) {
          rowObject.put(field.getName,
            new java.lang.Long(columnarBatch.getColumnVector(index).getLong(0)))
        } else if (dataType.isInstanceOf[BooleanType]) {
          rowObject.put(field.getName,
            new java.lang.Boolean(columnarBatch.getColumnVector(index).getBoolean(0)))
//        } else if (dataType.isInstanceOf[DoubleType]) {
//          rowObject.put(field.getName,
//            new java.lang.Double(columnarBatch.getColumnVector(index).getDouble(0)))
//        } else if (dataType.isInstanceOf[FloatType]) {
//          rowObject.put(field.getName,
//            new java.lang.Float(columnarBatch.getColumnVector(index).getFloat(0)))
        } else if (dataType.isInstanceOf[StringType]) {
          rowObject.put(field.getName,
            columnarBatch.getColumnVector(index).getString(0))
        } else {
          throw new UnsupportedOperationException(field.getDataType.toString)
        }
    }

    new ObjectMapper().writeValueAsString(rowObject)
  }

  private def convertJSONToRow(json: String, readSchema: StructType): Row = {
    try {
      val jsonNode = new ObjectMapper().readTree(json)
      new JsonRow(jsonNode.asInstanceOf[ObjectNode], readSchema)
    } catch {
      case ex: JsonProcessingException =>
        throw new RuntimeException(String.format("Could not parse JSON: %s", json), ex)
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
