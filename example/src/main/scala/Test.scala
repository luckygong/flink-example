import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.util.Random

/**
 * bin/flink  run -m yarn-cluster -yn 6 -yjm 1024 -ytm 2048 -c com.wacai.stanlee.job.RawCellInput  /data/program/flink-app/rawcell/hercules-1.0-SNAPSHOT.jar
 */
object Test {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new FsStateBackend("file:///Users/sjk/apps/db"))
    env.enableCheckpointing(5000)

    val source = new RichParallelSourceFunction[String]() {
      var bool = true

      override def cancel(): Unit = {
        bool = false
      }

      override def run(ctx: SourceContext[String]): Unit = {
        while (bool) {
          ctx.collect(newRow(300))
          Thread.sleep(100)
        }
      }
    }

    env
      .addSource(source).setParallelism(6)
      .map(s => (s.length, s)).setParallelism(6)
      .print()


    env.execute("cell raw data into phoenix")
  }

  private lazy val seed: Array[Char] = ('a' to 'z').map(f => f).toArray // ++ (0 until 10).map(_.toChar).toArray

  private def newRow(kb: Int): String = {
    val rd = Random.nextInt(Int.MaxValue)
    val seedLen = seed.length
    val len = kb * 1024
    (0 until len).map(f => seed((f + rd) % seedLen)).mkString
  }

}
