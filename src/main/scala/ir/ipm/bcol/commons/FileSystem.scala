package ir.ipm.bcol.commons

import java.io.File

class FileSystem {

  def getListOfFiles(dir: String, format: String): List[String] = {

    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      .filter(_.getName.endsWith(s".$format"))
      .map(_.toString).toList.sorted

  }

}
