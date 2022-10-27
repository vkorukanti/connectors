/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.standalone.internal

import scala.collection.mutable

import org.scalatest.FunSuite

import io.delta.standalone.exceptions.ColumnMappingUnsupportedException
import io.delta.standalone.types.{DataType, IntegerType, StringType, StructField, StructType}

import io.delta.standalone.internal.DeltaColumnMapping._
import io.delta.standalone.internal.actions.{Metadata, Protocol}


class DeltaColumnMappingSuite extends FunSuite {

  // Set the column mapping mode to 'id' or unknown and expect unsupported error
  test("unsupported column mapping mode") {
    val schema = struct(field("a", new IntegerType), field("b", new StringType))
    val ex = intercept[ColumnMappingUnsupportedException] {
      verifyAndUpdateMetadataChange(
        oldProtocol = protocol(1, 2),
        oldMetadata = metadata(schema, Map.empty),
        newMetadata = metadata(schema, withMode(Map.empty, "id")),
        isCreatingNewTable = false)
    }
    assert(ex.getMessage contains "The column mapping mode `id` is not supported for this " +
        "Delta version. Please upgrade if you want to use this mode.")
  }

  test("unsupported mode change: example: name to none") {
    val schema = struct(field("a", new IntegerType), field("b", new StringType))
    val ex = intercept[ColumnMappingUnsupportedException] {
      verifyAndUpdateMetadataChange(
        oldProtocol = protocol(1, 2),
        oldMetadata = metadata(schema, withMode(Map.empty, "name")),
        newMetadata = metadata(schema, withMode(Map.empty, "none")),
        isCreatingNewTable = false)
    }
    assert(ex.getMessage contains
        "Changing column mapping mode from name to none is not supported.")
  }

  test("unsupported protocol version in existing table and no protocol upgrade in new metadata") {
    val schema = struct(field("a", new IntegerType), field("b", new StringType))
    val ex = intercept[ColumnMappingUnsupportedException] {
      verifyAndUpdateMetadataChange(
        oldProtocol = protocol(1, 2),
        oldMetadata = metadata(schema, Map.empty),
        newMetadata = metadata(schema, withMode(Map.empty, "name")),
        isCreatingNewTable = false)
    }
    assert(ex.getMessage contains "Your current table protocol version does not support " +
        "changing column mapping modes using delta.columnMapping.mode.")
    assert(ex.getMessage contains
        "Required Delta protocol version for column mapping: Protocol(2,5)")
    assert(ex.getMessage contains
        "Your table's current Delta protocol version: Protocol(1,2)")
  }

  test("unsupported protocol version in existing table and in new metadata") {
    val schema = struct(field("a", new IntegerType), field("b", new StringType))
    val ex = intercept[ColumnMappingUnsupportedException] {
      verifyAndUpdateMetadataChange(
        oldProtocol = protocol(1, 2),
        oldMetadata = metadata(schema, Map.empty),
        newMetadata = metadata(schema,
                               withProtocol(withMode(Map.empty, "name"), readerV = 1, writerV = 5)),
        isCreatingNewTable = false)
    }
    assert(ex.getMessage contains "Your current table protocol version does not support " +
        "changing column mapping modes using delta.columnMapping.mode.")
    assert(ex.getMessage contains
        "Required Delta protocol version for column mapping: Protocol(2,5)")
    assert(ex.getMessage contains
        "Your table's current Delta protocol version: Protocol(1,2)")
  }

  test("change mode=name on an existing table") {
    val schema = struct(field("a", new IntegerType), field("b", new StringType))
    val updatedMetadata = verifyAndUpdateMetadataChange(
      oldProtocol = protocol(1, 2),
      oldMetadata = metadata(schema, Map.empty),
      newMetadata = metadata(schema,
                            withProtocol(withMode(Map.empty, "name"), readerV = 2, writerV = 5)),
      isCreatingNewTable = false)
    val newSchema = updatedMetadata.schema
    assert(newSchema.getFields.size == 2)
    assertColumnMappingMetadata(newSchema.get("a"), expectedId = 1, expectedPhyName = "a")
    assertColumnMappingMetadata(newSchema.get("b"), expectedId = 2, expectedPhyName = "b")
  }

  test("change mode=name on an new table") {
    val schema = struct(field("a", new IntegerType), field("b", new StringType))
    val updatedMetadata = verifyAndUpdateMetadataChange(
      oldProtocol = protocol(1, 2),
      oldMetadata = metadata(schema, Map.empty),
      newMetadata = metadata(schema,
                             withProtocol(withMode(Map.empty, "name"), readerV = 2, writerV = 5)),
      isCreatingNewTable = true)
    val newSchema = updatedMetadata.schema
    assert(newSchema.getFields.size == 2)
    assertColumnMappingMetadata(newSchema.get("a"), expectedId = 1, expectedPhyName = "UUID")
    assertColumnMappingMetadata(newSchema.get("b"), expectedId = 2, expectedPhyName = "UUID")
  }

  private def struct(fields: StructField *): StructType = new StructType(fields.toArray)

  private def field(name: String, dataType: DataType): StructField = new StructField(name, dataType)

  private def metadata(schema: StructType, conf: Map[String, String]): Metadata = {
    Metadata(
      schemaString = schema.toJson,
      configuration = conf
    )
  }

  private def protocol(readerVersion: Int, writerVersion: Int): Protocol =
    new Protocol(readerVersion, writerVersion)

  private def withMode(existingConf: Map[String, String], newMode: String): Map[String, String] = {
    val newConf = mutable.Map[String, String]() ++ existingConf
    newConf.put(DeltaConfigs.COLUMN_MAPPING_MODE.key, newMode)
    newConf.toMap
  }

  private def withProtocol(
      existingConf: Map[String, String], readerV: Int, writerV: Int): Map[String, String] = {
    val newConf = mutable.Map[String, String]() ++ existingConf
    newConf.put(Protocol.MIN_READER_VERSION_PROP, readerV.toString)
    newConf.put(Protocol.MIN_WRITER_VERSION_PROP, writerV.toString)
    newConf.toMap
  }

  def assertColumnMappingMetadata(
      field: StructField, expectedId: Long, expectedPhyName: String): Unit = {
    val actualMetadata = field.getMetadata()
    assert(actualMetadata.get(COLUMN_MAPPING_METADATA_ID_KEY) == expectedId)
    val actualPhyName = actualMetadata.get(COLUMN_MAPPING_PHYSICAL_NAME_KEY)
    if (expectedPhyName == "UUID") {
      assert(actualPhyName.toString.startsWith("col-"))
    } else {
      assert(actualMetadata.get(COLUMN_MAPPING_PHYSICAL_NAME_KEY) == expectedPhyName)
    }
  }
}
