import java.io.File

import com.google.common.base.Charsets
import com.google.common.io.Files

import scala.io.Source

object Test {
  def main(args: Array[String]): Unit = {

    //    mergeFiles("/Users/sjk/apps/data/learning-social-circles/egonets")
    mergeCSV("/Users/sjk/apps/data/learning-social-circles/result", "connected_conponents_result.csv")
  }

  def mergeCSV(directory: String, name: String): Unit = {
    val list: Array[String] = new File(directory).listFiles().filter(_.canRead).flatMap(f => {
      Source.fromFile(f).getLines()
    }).sorted

    val newf = new File(directory, name)
    Files.write(list.mkString("\n"), newf, Charsets.UTF_8)
  }

  //  data from https://www.kaggle.com/c/learning-social-circles
  def mergeFiles(directory: String): Unit = {
    val dir = new File(directory)
    val list: Array[String] = dir.listFiles().filter(_.canRead).flatMap(f => {
      Source.fromFile(f).getLines().flatMap(l => {
        val s1 = l.splitAt(l.indexOf(":"))
        val user = s1._1
        val list = s1._2.trim.drop(1).split(" ").map(_.trim).filter(_.nonEmpty)
        list.map(u => user + "	" + u)
      })
    })

    val newf = new File(dir.getParentFile, "egonets-all.csv")
    Files.write(list.mkString("\n"), newf, Charsets.UTF_8)
  }
}
