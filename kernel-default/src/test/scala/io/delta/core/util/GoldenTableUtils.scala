package io.delta.core.util

import java.io.File

trait GoldenTableUtils {

  private lazy val resourcesDirectory = {
    val dir = new File("src/test/resources").getCanonicalFile
    require(dir.exists())
    dir
  }

  def withGoldenTable(tableName: String)(testFunc: String => Unit): Unit = {
    val tablePath = new File(resourcesDirectory, tableName).getCanonicalPath
    testFunc(tablePath)
  }

}
