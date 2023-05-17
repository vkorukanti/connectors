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

package io.delta.kernel.util

import java.io.File
import java.nio.file.Paths

import org.apache.hadoop.fs.Path

trait GoldenTableUtils {

  private lazy val resourcesDirectory = {
    val cwd = Paths.get(".").toAbsolutePath.toString

    val resourcesPath = if (cwd.endsWith("connectors/.")) {
      // intellij will see CWD as <path-to-connectors/.
      new Path(cwd, "kernel-default/src/test/resources")
    } else {
      // SBT CLI will see CWD as <path-to-connectors>/kernel-default/.
      new Path(cwd, "src/test/resources")
    }

    val dir = new File(resourcesPath.toString)
    require(dir.exists())
    dir
  }

  def withGoldenTable(tableName: String)(testFunc: String => Unit): Unit = {
    val tablePath = new File(resourcesDirectory, tableName).getCanonicalPath
    testFunc(tablePath)
  }

}
