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

import io.delta.standalone.types.{ArrayType, FieldMetadata, MapType, StructField, StructType}

import io.delta.standalone.internal.actions.Protocol
import io.delta.standalone.internal.exception.DeltaErrors

object GeneratedColumn {
  /** Minimum writer protocol version needed in order to support generated columns. */
  private val MIN_WRITER_VERSION = 4

  /**
   * The metadata key recording the generation expression in a generated column's [[StructField]].
   */
  val GENERATION_EXPRESSION_METADATA_KEY = "delta.generationExpression"

  def satisfyGeneratedColumnProtocol(protocol: Protocol): Boolean = {
    protocol.minWriterVersion >= MIN_WRITER_VERSION
  }

  /** Whether the field contains the generation expression. */
  private def isGeneratedColumn(field: StructField): Boolean = {
    field.getMetadata.contains(GENERATION_EXPRESSION_METADATA_KEY)
  }
  /**
   * Whether any generation expressions exist in the schema. Note: this doesn't mean the table
   * contains generated columns. A table has generated columns only if its
   * `minWriterVersion` >= `GeneratedColumn.MIN_WRITER_VERSION` and some of columns in the table
   * schema contain generation expressions. Use `enforcesGeneratedColumns` to check generated
   * column tables instead.
   */
  def hasGeneratedColumns(schema: StructType): Boolean = {
    schema.getFields.contains(field => isGeneratedColumn(field))
  }

  /**
   * Verify that the generated columns are only the top level columns.
   *
   * Note: Generation expression verification is not done to make sure it is valid and refers
   * to valid columns in the table. This is mainly because the Standalone APIs don't have
   * the capability to parse the generation expression.
   */
  def verifyGeneratedColumns(schema: StructType): Unit = {
    def assertNoGeneratedColumnExpression(parentPath: Seq[String], field: StructField): Unit = {
      val columnPath = parentPath :+ field.getName
      if (isGeneratedColumn(field) && columnPath.size > 1) {
        throw DeltaErrors.generatedColumnOnlyOnTopLevelColumns(columnPath)
      }
      field.getDataType match {
        case s: StructType =>
          s.getFields.foreach(
          field => assertNoGeneratedColumnExpression(columnPath, field))
        case _ => // no-op
      }
    }

    schema.getFields.foreach(field => assertNoGeneratedColumnExpression(Seq.empty, field))
  }

  /** Return the generation expression from a field metadata if any. */
  def getGenerationExpressionStr(metadata: FieldMetadata): Option[String] = {
    if (metadata.contains(GENERATION_EXPRESSION_METADATA_KEY)) {
      Some(metadata.get(GENERATION_EXPRESSION_METADATA_KEY).toString)
    } else {
      None
    }
  }
}
