package kafka

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
 * consume kafka topic
 * Created by sjk on 11/23/16.
 */
object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    // parse input arguments
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)

    if (parameterTool.getNumberOfParameters < 4) {
      System.out.println("Missing parameters!\nUsage: Kafka --topic <topic> " + "--bootstrap.servers <kafka brokers> --zookeeper.connect <zk quorum> --group.id <some id>")
      return
    }

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    env.enableCheckpointing(5000) // create a checkpoint every 5 seconds
    env.getConfig.setGlobalJobParameters(parameterTool) // make parameters available in the web interface
    val consumer: FlinkKafkaConsumer09[String] = new FlinkKafkaConsumer09[String](parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties)
    val messageStream: DataStream[String] = env.addSource(consumer)

    // write kafka stream to standard out.
    messageStream.print

    env.execute("Read from Kafka example")


  }
}
