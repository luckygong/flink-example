package rocksdb

import java.util.Properties

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.ConfigConstants
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.Checkpointed
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector


/**
 * checkpoint, state backend use RocksDB
 * Created by sjk on 11/18/16.
 */
object CheckpointApp {
  def main(args: Array[String]): Unit = {
    val storeDir = "/Users/sjk/apps/db"
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "localhost:9092")
    prop.setProperty("zookeeper.connect", "localhost:2181")
    prop.setProperty("group.id", "bench-" + System.currentTimeMillis())
    prop.setProperty(ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD, "true")
    prop.setProperty(ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS, "3000")
    prop.setProperty(ConfigConstants.CHECKPOINTS_DIRECTORY_KEY, s"file://$storeDir/checkpoint")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(4000)
      .setStateBackend(new RocksDBStateBackend("file://" + storeDir))
      .setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.getConfig.enableForceKryo()
    env.setParallelism(2)

    val s1 = env.addSource(new SourceFunction[String]() {
      private var isRunning: Boolean = true

      override def cancel(): Unit = {
        isRunning = false
      }

      override def run(ctx: SourceContext[String]): Unit = {
        var i = 0
        while (true) {
          ctx.collect(KafkaProduceMain.newLine(i))
          i = i + 1
          if (i % 100 == 0) {
            Thread.sleep(5000)
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
      .flatMap(new MyFlatMapCkpWindow)
      .print()

    println(env.getExecutionPlan)

    env.execute()
  }
}

final case class Item(name: String, price: Int, timestamp: Long) {
  override def toString: String = s"$name,$price,$timestamp"
}

class MyFlatMapCkpWindow extends RichFlatMapFunction[Item, Tuple2[String, Long]] with Checkpointed[Tuple2[String, Long]] {
  private var userSum: Tuple2[String, Long] = new Tuple2("", 0L)

  override def flatMap(input: Item, out: Collector[Tuple2[String, Long]]): Unit = {
    val bool = userSum.f0 == input.name

    userSum.f0 = input.name
    userSum.f1 += input.price

    if (!bool) {
      out.collect(userSum)
    }
  }

  override def snapshotState(checkpointId: Long, checkpointTimestamp: Long): Tuple2[String, Long] = {
    userSum
  }

  override def restoreState(state: Tuple2[String, Long]): Unit = {
    userSum = state
  }
}