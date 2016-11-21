package rocksdb

import java.util.Properties

object KafkaProduceMain {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.put("metadata.broker.list", "localhost:9092")
    properties.put("serializer.class", "kafka.serializer.StringEncoder")
    properties.put("request.required.acks", "1")
    //    producer = new Producer(new ProducerConfig(properties))

  }
}
