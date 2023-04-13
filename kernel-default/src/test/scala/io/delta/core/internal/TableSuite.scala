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

package io.delta.core.internal

import scala.collection.JavaConverters._

import io.delta.core.Table
import io.delta.core.expressions.{And, EqualTo, Literal}
import io.delta.core.helpers.{DefaultConnectorReadContext, DefaultTableHelper}
import io.delta.core.util.GoldenTableUtils
import org.scalatest.funsuite.AnyFunSuite

// scalastyle:off println
class TableSuite extends AnyFunSuite with GoldenTableUtils {

  test("can load latest table version - with a checkpoint") {
    withGoldenTable("basic-with-checkpoint") { path =>
      val table = Table.forPath(path, new DefaultTableHelper())
      val snapshot = table.getLatestSnapshot
      assert(snapshot.getVersion === 14)
    }
  }

  test("can load latest table version - without checkpoint") {
    withGoldenTable("basic-no-checkpoint") { path =>
      val table = Table.forPath(path, new DefaultTableHelper())
      val snapshot = table.getLatestSnapshot
      assert(snapshot.getVersion === 8)
    }
  }

  test("can create scan tasks - without a checkpoint") {
    withGoldenTable("basic-no-checkpoint") { path =>
      val table = Table.forPath(path, new DefaultTableHelper())
      val snapshot = table.getLatestSnapshot

      var count = 0
      val iter = snapshot.getScanBuilder().build().getTasks()
      while (iter.hasNext) {
        val task = iter.next()
        val data = task.getData(
          DefaultConnectorReadContext.of(),
          snapshot.getSchema
        )
        while (data.hasNext) {
          val batch = data.next();
        }
        count += 1
      }
      assert(count === 18)
      iter.close()
    }
  }

  test("can load table protocol & schema - table without a checkpoint") {
    withGoldenTable("basic-no-checkpoint") { path =>
      val table = Table.forPath(path, new DefaultTableHelper())
      val snapshot = table.getLatestSnapshot.asInstanceOf[SnapshotImpl]
      println(snapshot.getSchema)
      println(snapshot.getProtocol)
    }
  }

  test("can parse add file partition values - basic - no checkpoint") {
    withGoldenTable("basic-partitioned-no-checkpoint") { path =>
      val table = Table.forPath(path, new DefaultTableHelper())
      val snapshot = table.getLatestSnapshot.asInstanceOf[SnapshotImpl]
      snapshot.getAddFiles.forEachRemaining(x => println(x))
    }
  }

  test("can perform partition pruning - basic - no checkpoint") {
    withGoldenTable("basic-partitioned-no-checkpoint") { path =>
      val table = Table.forPath(path, new DefaultTableHelper())
      val snapshot = table.getLatestSnapshot.asInstanceOf[SnapshotImpl]
      val schema = snapshot.getSchema

      val partitionFilter1 = new EqualTo(schema.column("part_a"), Literal.of(0L));
      val scan1 = snapshot.getScanBuilder().withFilter(partitionFilter1).build()
      scan1
        .getTasks
        .asScala
        .map(task => task.asInstanceOf[ScanTaskImpl].getAddFile)
        .foreach(add => assert(add.getPartitionValues.get("part_a").toLong == 0))

      val partitionFilter2 = new And(
        new EqualTo(schema.column("part_a"), Literal.of(0L)),
        new EqualTo(schema.column("part_b"), Literal.of(0L))
      )
      val scan2 = snapshot.getScanBuilder().withFilter(partitionFilter2).build()
      scan2
        .getTasks
        .asScala
        .map(task => task.asInstanceOf[ScanTaskImpl].getAddFile)
        .foreach { add => assert(
            add.getPartitionValues.get("part_a").toLong == 0 &&
            add.getPartitionValues.get("part_b").toLong == 0
          )
        }
    }
  }
}
