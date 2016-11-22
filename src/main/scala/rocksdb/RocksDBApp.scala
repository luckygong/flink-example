package rocksdb

import java.util
import java.util.Properties

import org.apache.flink.api.common.functions.{FoldFunction, RichFlatMapFunction}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.Checkpointed
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
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "localhost:9092")
    prop.setProperty("zookeeper.connect", "localhost:2181")
    prop.setProperty("group.id", "bench")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new RocksDBStateBackend("file:///Users/sjk/apps/db"))
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.enableCheckpointing(3000)
    val topic = "msg"
    val consumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), prop)
    val stream: DataStream[String] = env.addSource(consumer)
    stream
      .map(f => {
        val array = f.split(",")
        Item(array.head.trim, array(1).trim.toInt, array(2).trim.toLong)
      })
      .keyBy(_.name)
      .window(TumblingEventTimeWindows.of(Time.minutes(10)))
      .fold(Map.empty[String, Int], new MyStateWindow())
      .print()

    //      .flatMap(new MyMapStateWindow).print()

    //      .flatMapWithState((item: Item, map: Option[Map[String, Set[Item]]]) => {
    //        println("===> " + item)
    //        map match {
    //          case Some(m) =>
    //            val list = m.getOrElse(item.name, Set.empty[Item]) ++ Set(item)
    //            (Iterator(item), Some(m ++ Map(item.name -> list)))
    //          case None =>
    //            (Iterator(item), Some(Map(item.name -> Set(item))))
    //        }
    //      })
    //      .print()
    println(env.getExecutionPlan)
    env.execute()
  }
}

final case class Item(name: String, price: Int, timestamp: Long)

class MyStateWindow extends FoldFunction[Item, Map[String, Int]] with Checkpointed[util.HashMap[String, Int]] {
  @transient private var map = new util.HashMap[String, Int]()

  override def fold(accumulator: Map[String, Int], value: Item): Map[String, Int] = {
    accumulator.get(value.name) match {
      case Some(sum) => accumulator ++ Map(value.name -> sum)
      case None => accumulator ++ Map(value.name -> 1)
    }
  }

  override def snapshotState(checkpointId: Long, checkpointTimestamp: Long): util.HashMap[String, Int] = map

  override def restoreState(state: util.HashMap[String, Int]): Unit = {
    map = state
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

  override def snapshotState(checkpointId: Long, checkpointTimestamp: Long): Tuple2[String, Int] = userSum

  override def restoreState(state: Tuple2[String, Int]): Unit = {
    userSum = state
  }
}