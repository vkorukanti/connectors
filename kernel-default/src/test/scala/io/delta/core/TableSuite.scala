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

package io.delta.core

import io.delta.core.helpers.DefaultTableHelper
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
        task.getData
        count += 1
      }
      assert(count === 18)
      iter.close()
    }
  }

  test("can load table schema - table without a checkpoint") {
    withGoldenTable("basic-no-checkpoint") { path =>
      val table = Table.forPath(path, new DefaultTableHelper())
      val snapshot = table.getLatestSnapshot
      println(snapshot.getSchema)
    }
  }
}
