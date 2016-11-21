import org.rocksdb.{FlushOptions, Options, RocksDB}

/**
 * rocksDB state backend test
 * Created by sjk on 11/14/16.
 */
object RocksDBStateTest {

  private lazy val id = 1L

  def main(args: Array[String]): Unit = {
    val dbPath: String = "/Users/sjk/apps/db"

    //    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    //    env.socketTextStream()
    //    val checkpointPath: String = "/Users/sjk/apps/ckt"
    //    val backend: RocksDBStateBackend = new RocksDBStateBackend(checkpointPath)
    //    backend.setDbStoragePath(dbPath)


    RocksDB.loadLibrary()
    val options = new Options().setCreateIfMissing(true)
    val db = RocksDB.open(options, dbPath)
    if (db != null) db.close()
    options.dispose()

    val (k, v) = buildKV(1)
    (0 until 1024).foreach(f => {
      db.put(k, v)
      if (f % 10000 == 0) {
        db.flush(new FlushOptions().setWaitForFlush(true))
      }
    })

  }

  def buildKV(kb: Int): (Array[Byte], Array[Byte]) = {
    val key = long2bytes(id)
    val count = kb * 1024 / 8 - 1
    val value: Array[Byte] = (0 until count).flatMap(f => long2bytes(id + 1)).toArray
    //    println(key.length + value.length)
    (key, value)
  }

  def test(): Unit = {
    (0L until 1000L).foreach(f => {
      val l = long2bytes(f)
      val b = bytes2long(l)
      println(s"$f $b ${f == b}")
    })
  }

  def bytes2long(value: Array[Byte]): Long = {
    var result: java.lang.Long = 0L
    for (i <- 0 until 8) {
      result <<= 8
      result |= (value(i) & 0xFF)
    }
    result
  }

  def long2bytes(value: Long): Array[Byte] = {
    Array[Byte](
      (value >> 56).toByte,
      (value >> 48).toByte,
      (value >> 40).toByte,
      (value >> 32).toByte,
      (value >> 24).toByte,
      (value >> 16).toByte,
      (value >> 8).toByte,
      value.toByte
    )
  }
}
