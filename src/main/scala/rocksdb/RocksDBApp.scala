package rocksdb

import java.util.Properties

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
 * checkpoint, state backend use RocksDB
 * Created by sjk on 11/18/16.
 */
object RocksDBApp {
  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "localhost:9092")
    // only required for Kafka 0.8
    prop.setProperty("zookeeper.connect", "localhost:2181")
    prop.setProperty("group.id", "bench")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new RocksDBStateBackend("file:///Users/sjk/apps/db"))
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val consumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String]("topic", new SimpleStringSchema(), prop)
    val stream: DataStream[String] = env.addSource(consumer)
    stream
      .map(f => {
        val array = f.split(",")
        Item(array.head.trim, array(1).trim.toInt, array(2).trim.toLong)
      })
      .keyBy(_.name)
      .flatMapWithState((item: Item, map: Option[Map[String, Set[Item]]]) => {
        map match {
          case Some(m) =>
            val list = m.getOrElse(item.name, Set.empty[Item]) ++ Set(item)
            (Iterator(item), Some(m ++ Map(item.name -> list)))
          case None =>
            (Iterator(item), Some(Map(item.name -> Set(item))))
        }
      })
      .print()
    println(env.getExecutionPlan)
    env.execute()
  }
}

final case class Item(name: String, price: Int, timestamp: Long)

