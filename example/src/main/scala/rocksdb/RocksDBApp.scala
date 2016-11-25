package rocksdb

import java.io.File
import java.util
import java.util.{Collections, Properties}

import com.google.common.base.Charsets
import com.google.common.io.Files
import org.apache.flink.api.common.functions.{FoldFunction, RichFlatMapFunction}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.Checkpointed
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector


/**
 * checkpoint, state backend use RocksDB
 * Created by sjk on 11/18/16.
 */
object RocksDBApp {
  def main(args: Array[String]): Unit = {
    val storeDir = "/Users/sjk/apps/db"
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "localhost:9092")
    prop.setProperty("zookeeper.connect", "localhost:2181")
    prop.setProperty("group.id", "bench-" + System.currentTimeMillis())
    prop.setProperty(ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD, "true")
    prop.setProperty(ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS, "3000")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(2000)
    env.setStateBackend(new RocksDBStateBackend("file://" + storeDir))
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.getConfig.enableForceKryo()
    env.setParallelism(8)

    val topic = "msg"
    val consumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), prop)
    val stream: DataStream[String] = env.addSource(consumer).setParallelism(2).rebalance
    val s1 = env.addSource(new SourceFunction[String]() {
      //  todo @transient isRunning is false when running
      private var isRunning: Boolean = true

      override def cancel(): Unit = {
        isRunning = false
      }

      override def run(ctx: SourceContext[String]): Unit = {
        var i = 0
        while (true) {
          ctx.collect(KafkaProduceMain.newLine(i))
          i = i + 1
          if (i % 10000 == 0) {
            Thread.sleep(2000)
          }
        }
      }
    })

    s1
      .map(f => {
        val array = f.split(",")
        Item(array.head.trim, array(1).trim.toInt, array(2).trim.toLong)
      })
      .keyBy(_.name)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .fold(new util.ArrayList[String](), new MyStateWindow(storeDir))
      .print()
    println(env.getExecutionPlan)
    env.execute()
  }
}

final case class Item(name: String, price: Int, timestamp: Long) {
  override def toString: String = s"$name,$price,$timestamp"
}

class MyStateWindow(storeDir: String) extends FoldFunction[Item, util.ArrayList[String]] with Checkpointed[util.ArrayList[String]] {
  private var list = new util.ArrayList[String](102400)

  override def fold(accumulator: util.ArrayList[String], value: Item): util.ArrayList[String] = {
    accumulator.add(value.toString)
    if (accumulator != null)
      list.addAll(accumulator)
    else
      list.add(value.toString)

    accumulator
  }

  override def snapshotState(checkpointId: Long, checkpointTimestamp: Long): util.ArrayList[String] = {
    val ret = new util.ArrayList[String](list.size())
    val file = new File(s"$storeDir/raw/${System.currentTimeMillis()}")
    file.getParentFile.mkdirs()

    val it: util.Iterator[String] = ret.iterator()
    while (it.hasNext) {
      val str = it.next()
      Files.append(str.toString + "\n", file, Charsets.UTF_8)
    }

    Collections.copy(ret, list)
    list.clear()
    ret
  }

  override def restoreState(state: util.ArrayList[String]): Unit = {
    list.clear()
    list = state
  }
}

class MyMapStateWindow extends RichFlatMapFunction[Item, Tuple2[String, Int]] with Checkpointed[Tuple2[String, Int]] {

  @transient private var userSum: Tuple2[String, Int] = _

  override def flatMap(input: Item, out: Collector[Tuple2[String, Int]]): Unit = {
    userSum.f1 += 1
    out.collect(userSum)
  }

  override def open(config: Configuration): Unit = {
    userSum = Tuple2.of("", 0)
  }

  override def snapshotState(checkpointId: Long, checkpointTimestamp: Long): Tuple2[String, Int] = {
    userSum
  }

  override def restoreState(state: Tuple2[String, Int]): Unit = {
    userSum = state
  }
}