//package rocksdb
//
//import java.lang.Iterable
//import java.util
//import java.util.Properties
//
//import org.apache.flink.api.common.functions.{Partitioner, RichFlatMapFunction}
//import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
//import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
//import org.apache.flink.api.java.tuple.Tuple2
//import org.apache.flink.configuration.{ConfigConstants, Configuration}
//import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
//import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
//import org.apache.flink.streaming.api.collector.selector.OutputSelector
//import org.apache.flink.streaming.api.functions.source.SourceFunction
//import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
//import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
//import org.apache.flink.util.Collector
//
//
///**
// * checkpoint, state backend use RocksDB
// * Created by sjk on 11/18/16.
// */
//object RocksDBApp {
//  def main(args: Array[String]): Unit = {
//    val storeDir = "/Users/sjk/apps/db"
//    val prop = new Properties()
//    prop.setProperty("bootstrap.servers", "localhost:9092")
//    prop.setProperty("zookeeper.connect", "localhost:2181")
//    prop.setProperty("group.id", "bench-" + System.currentTimeMillis())
//    prop.setProperty(ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD, "true")
//    prop.setProperty(ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS, "3000")
//    prop.setProperty(ConfigConstants.CHECKPOINTS_DIRECTORY_KEY, "file:///Users/sjk/apps/db/checkpoint")
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.enableCheckpointing(4000)
//      .setStateBackend(new RocksDBStateBackend("file://" + storeDir))
//      .setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
//    env.getConfig.enableForceKryo()
//    env.setMaxParallelism(2)
//
//    //    val topic = "msg"
//    //    val consumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), prop)
//    //    val stream: DataStream[String] = env.addSource(consumer).setParallelism(2).rebalance
//    val partby = new Partitioner[String]() {
//      private var roundRobin: Int = 0
//
//      override def partition(key: String, numPartitions: Int): Int = {
//        roundRobin = roundRobin + 1
//        if (roundRobin % 2048 == 0) {
//          roundRobin = 0
//        }
//        roundRobin % numPartitions
//      }
//    }
//
////    val myselector = new OutputSelector[Tuple2[String, Long]] {
////      override def select(value: Tuple2[String, Long]): Iterable[Tuple2[String, Long]] = {
////        val ret = new util.ArrayList[Tuple2[String, Long]]()
////        ret.add(value)
////        ret
////      }
////    }
//
//    env
//      .addSource(new TestSource)
//      .map(f => {
//        val array = f.split(",")
//        Item(array.head.trim, array(1).trim.toInt, array(2).trim.toLong)
//      })
//      .keyBy(_.name)
//      .flatMap(new MyFlatMapStateWindow).partitionCustom(partby, 0)
//      .map(f => (f.f0, f.f1 + 1)).shuffle
//      .filter(_._2 > 0).rescale
//      .print()
//
//    println(env.getExecutionPlan)
//
//    env.execute()
//  }
//}
//
//class TestSource extends SourceFunction[String] {
//  private var isRunning: Boolean = true
//
//  override def cancel(): Unit = isRunning = false
//
//  override def run(ctx: SourceContext[String]): Unit = {
//    var i = 0
//    while (true) {
//      ctx.collect(KafkaProduceMain.newLine(i))
//      i = i + 1
//      if (i % 100 == 0) {
//        Thread.sleep(5000)
//      }
//    }
//  }
//}
//
//class MyFlatMapStateWindow extends RichFlatMapFunction[Item, Tuple2[String, Long]] with CheckpointedFunction {
//  private var userSum: ValueState[Tuple2[String, Long]] = _
//
//  override def flatMap(input: Item, out: Collector[Tuple2[String, Long]]): Unit = {
//    val v = userSum.value()
//    val bool = v.f0 == input.name
//
//    v.f0 = input.name
//    v.f1 += input.price
//    userSum.update(v)
//
//    if (!bool) {
//      out.collect(v)
//    }
//  }
//
//  override def open(config: Configuration): Unit = {
//    val descriptor = new ValueStateDescriptor(
//      "average",
//      TypeInformation.of(new TypeHint[Tuple2[String, Long]]() {}), // type information
//      Tuple2.of("", 0L) // default value of the state, if nothing was set
//    )
//    userSum = getRuntimeContext.getState(descriptor)
//  }
//
//  override def initializeState(context: FunctionInitializationContext): Unit = {
//    context.getKeyedStateStore
//  }
//
//  override def snapshotState(context: FunctionSnapshotContext): Unit = {
//    context
//  }
//}