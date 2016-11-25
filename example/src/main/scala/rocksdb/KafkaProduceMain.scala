package rocksdb

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object KafkaProduceMain {

  private lazy val producer: KafkaProducer[String, String] = initProducer()
  val itemNames = Array("Zhang San", "Kevin", "Li Si", "Li Lei", "Wang Gang", "Lee Sir")

  def main(args: Array[String]): Unit = {

    val topic = "msg"

    (0 until 10000000).foreach(f => {
      val ct = System.currentTimeMillis()
      val payload = newLine(f, ct)
      val msg: ProducerRecord[String, String] = new ProducerRecord(topic, payload)
      producer.send(msg)
      if (ct % 5000 == 0) {
        println(payload)
        (0 until 100).foreach(p => {
          val s = s"Kevin,$p,${System.currentTimeMillis()}"
          val kmsg: ProducerRecord[String, String] = new ProducerRecord(topic, s)
          producer.send(kmsg)
        })
      }
    })
  }

  def newLine(i: Int, ct: Long = System.currentTimeMillis()): String = {
    val ct = System.currentTimeMillis()
    val idx = (ct % itemNames.length).toInt
    s"${itemNames(idx)},${i % itemNames.length},$ct"
  }

  private def initProducer(host: String = "localhost:9092"): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", host)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    val producer: KafkaProducer[String, String] = new KafkaProducer(props)
    producer
  }
}
