package io.delta.core

import java.util.Optional

import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.delta.core.data.{ColumnarBatch, ColumnVector, JsonRow, Row}
import io.delta.core.expressions.{Expression, Literal}
import io.delta.core.helpers.{DefaultScanFileContext, DefaultTableHelper, ScanFileContext}
import io.delta.core.types._
import io.delta.core.util.GoldenTableUtils
import org.scalatest.funsuite.AnyFunSuite

class DeltaCoreAPISuite extends AnyFunSuite with GoldenTableUtils {
  test("end-to-end usage: reading a table") {
    withGoldenTable("delta-table") { path =>
      val table = Table.forPath(path, DefaultTableHelper.create())
      val snapshot = table.getLatestSnapshot

      // Contains both the data schema and partition schema
      val tableSchema = snapshot.getSchema

      // Go through the tableSchema and select the columns interested in reading
      val readSchema = new StructType().add("id", LongType.INSTANCE)
      val filter = Literal.TRUE

      val scanObject = scan(snapshot, readSchema, filter)

      val fileIter = scanObject.getScanFiles
      val scanState = scanObject.getScanState;

      // There should be just one element in the scan state
      val serializedScanState = convertColumnarBatchRowToJSON(scanState)

      val testScanFileContext = new DefaultScanFileContext

      val actualValueColumnValues = ArrayBuffer[Long]()
      while(fileIter.hasNext) {
        val fileColumnarBatch = fileIter.next()
        Seq.range(start = 0, end = fileColumnarBatch.getSize).foreach(rowId => {
          val serializedFileInfo = convertColumnarBatchRowToJSON(fileColumnarBatch, rowId)

          // START OF THE CODE THAT WILL BE EXECUTED ON THE EXECUTOR
          val dataBatches = ScanFileReader.readData(
            convertJSONToRow(serializedFileInfo, fileColumnarBatch.getSchema),
            convertJSONToRow(serializedScanState, scanState.getSchema),
            Optional.of(testScanFileContext),
            DefaultTableHelper.create(),
            readSchema
          )

          while(dataBatches.hasNext) {
            val batch = dataBatches.next()
            val valueColVector = batch.getColumnVector(0)
            actualValueColumnValues.append(vectorToLongs(valueColVector): _*)
          }
          // END OF THE CODE THAT WILL BE EXECUTED ON THE EXECUTOR
        })
      }
      assert(actualValueColumnValues.toSet === Seq.range(start = 0, end = 20).toSet)
    }
  }

  test("end-to-end usage: reading a table with checkpoint") {
    withGoldenTable("basic-with-checkpoint") { path =>
      val table = Table.forPath(path, DefaultTableHelper.create())
      val snapshot = table.getLatestSnapshot

      // Contains both the data schema and partition schema
      val tableSchema = snapshot.getSchema

      // Go through the tableSchema and select the columns interested in reading
      val readSchema = new StructType().add("id", LongType.INSTANCE)
      val filter = Literal.TRUE

      val scanObject = scan(snapshot, readSchema, filter)

      val fileIter = scanObject.getScanFiles
      val scanState = scanObject.getScanState;

      // There should be just one element in the scan state
      val serializedScanState = convertColumnarBatchRowToJSON(scanState)

      val testScanFileContext = new DefaultScanFileContext

      val actualValueColumnValues = ArrayBuffer[Long]()
      while(fileIter.hasNext) {
        val fileColumnarBatch = fileIter.next()
        Seq.range(start = 0, end = fileColumnarBatch.getSize).foreach(rowId => {
          val serializedFileInfo = convertColumnarBatchRowToJSON(fileColumnarBatch, rowId)

          // START OF THE CODE THAT WILL BE EXECUTED ON THE EXECUTOR
          val dataBatches = ScanFileReader.readData(
            convertJSONToRow(serializedFileInfo, fileColumnarBatch.getSchema),
            convertJSONToRow(serializedScanState, scanState.getSchema),
            Optional.of(testScanFileContext),
            DefaultTableHelper.create(),
            readSchema
          )

          while(dataBatches.hasNext) {
            val batch = dataBatches.next()
            val valueColVector = batch.getColumnVector(0)
            actualValueColumnValues.append(vectorToLongs(valueColVector): _*)
          }
          // END OF THE CODE THAT WILL BE EXECUTED ON THE EXECUTOR
        })
      }
      assert(actualValueColumnValues.toSet === Seq.range(start = 0, end = 20).toSet)
    }
  }

  private def convertColumnarBatchRowToJSON(columnarBatch: ColumnarBatch, rowIndex: Int): String = {
    val rowObject = new java.util.HashMap[String, Object]()

    import scala.collection.JavaConverters._
    val schema = columnarBatch.getSchema
    schema.fields().asScala.zipWithIndex.foreach {
      case (field, index) =>
        val dataType = field.getDataType
        if (dataType.isInstanceOf[StructType]) {
          rowObject.put(field.getName, columnarBatch.getColumnVector(index).getStruct(rowIndex))
        } else if (dataType.isInstanceOf[ArrayType]) {
          rowObject.put(field.getName, columnarBatch.getColumnVector(index).getArray(rowIndex))
        } else if (dataType.isInstanceOf[MapType]) {
          rowObject.put(field.getName, columnarBatch.getColumnVector(index).getMap(rowIndex))
        } else if (dataType.isInstanceOf[IntegerType]) {
          rowObject.put(field.getName,
            new Integer(columnarBatch.getColumnVector(index).getInt(rowIndex)))
        } else if (dataType.isInstanceOf[LongType]) {
          rowObject.put(field.getName,
            new java.lang.Long(columnarBatch.getColumnVector(index).getLong(rowIndex)))
        } else if (dataType.isInstanceOf[BooleanType]) {
          rowObject.put(field.getName,
            new java.lang.Boolean(columnarBatch.getColumnVector(index).getBoolean(rowIndex)));
        } else if (dataType.isInstanceOf[StringType]) {
          rowObject.put(field.getName,
            columnarBatch.getColumnVector(index).getString(rowIndex))
        } else {
          throw new UnsupportedOperationException(field.getDataType.toString)
        }
    }
    new ObjectMapper().writeValueAsString(rowObject)
  }

  private def convertColumnarBatchRowToJSON(row: Row): String = {
    val rowObject = new java.util.HashMap[String, Object]()

    import scala.collection.JavaConverters._
    val schema = row.getSchema
    schema.fields().asScala.zipWithIndex.foreach {
      case (field, index) =>
        val dataType = field.getDataType
        if (dataType.isInstanceOf[StructType]) {
          rowObject.put(field.getName, row.getRecord(index))
        } else if (dataType.isInstanceOf[ArrayType]) {
          rowObject.put(field.getName, row.getList(index))
        } else if (dataType.isInstanceOf[MapType]) {
          rowObject.put(field.getName, row.getMap(index))
        } else if (dataType.isInstanceOf[IntegerType]) {
          rowObject.put(field.getName, new Integer(row.getInt(index)))
        } else if (dataType.isInstanceOf[LongType]) {
          rowObject.put(field.getName, new java.lang.Long(row.getLong(index)))
        } else if (dataType.isInstanceOf[BooleanType]) {
          rowObject.put(field.getName, new java.lang.Boolean(row.getBoolean(index)))
//        } else if (dataType.isInstanceOf[DoubleType]) {
//          rowObject.put(field.getName,
//            new java.lang.Double(columnarBatch.getColumnVector(index).getDouble(0)))
//        } else if (dataType.isInstanceOf[FloatType]) {
//          rowObject.put(field.getName,
//            new java.lang.Float(columnarBatch.getColumnVector(index).getFloat(0)))
        } else if (dataType.isInstanceOf[StringType]) {
          rowObject.put(field.getName, row.getString(index))
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

  private def scan(snapshot: Snapshot, readSchema: StructType, filter: Expression = null): Scan = {
    var builder = snapshot.getScanBuilder()
    if (filter != null) {
      val builderFilterTuple = builder.applyFilter(filter)
      builder = builderFilterTuple._1
    }
    builder.build()
  }

  private def vectorToInts(intColumnVector: ColumnVector): Seq[Int] = {
    Seq.range(start = 0, end = intColumnVector.getSize)
      .map(rowId => intColumnVector.getInt(rowId))
      .toSeq
  }

  private def vectorToLongs(longColumnVector: ColumnVector): Seq[Long] = {
    Seq.range(start = 0, end = longColumnVector.getSize)
      .map(rowId => longColumnVector.getLong(rowId))
      .toSeq
  }
}
