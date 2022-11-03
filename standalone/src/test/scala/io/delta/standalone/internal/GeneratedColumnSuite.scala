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

import org.scalatest.FunSuite

import io.delta.standalone.types.{ArrayType, DataType, FloatType, IntegerType, LongType, MapType, StringType, StructField, StructType}

import io.delta.standalone.internal.actions.{Metadata, Protocol}

// WIP
class GeneratedColumnSuite extends FunSuite {
  test("generated column in a nested column") {

  }

  private def protocol(readerVersion: Int, writerVersion: Int): Protocol =
    new Protocol(readerVersion, writerVersion)

  private val testSchema = struct(
    field("i", new IntegerType),
    field("str", new StringType),
    field("m", map(new FloatType, new StringType)),
    field("ms", map(
      new FloatType,
      struct(field("msi", new IntegerType), field("msf", new FloatType)))),
    field("a", array(new LongType)),
    field("s",
          struct(
            field("sa", new IntegerType),
            field("ss", struct(field("ssstr", new StringType))))))

  private def struct(fields: StructField *): StructType = new StructType(fields.toArray)

  private def map(keyType: DataType, valueType: DataType): MapType =
    new MapType(keyType, valueType, true)

  private def array(elementType: DataType): ArrayType = new ArrayType(elementType, true)

  private def field(name: String, dataType: DataType): StructField = new StructField(name, dataType)

  private def metadata(schema: StructType, conf: Map[String, String]): Metadata = {
    Metadata(
      schemaString = schema.toJson,
      configuration = conf)
  }
}
