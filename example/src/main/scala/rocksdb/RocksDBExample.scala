package rocksdb

import org.rocksdb.{FlushOptions, Options, RocksDB}
import util.MsgBuilder

/**
 * rocksDB state backend test
 * Created by sjk on 11/14/16.
 */
object RocksDBExample {


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

    val (k, v) = MsgBuilder.buildKV(1)
    (0 until 1024).foreach(f => {
      db.put(k, v)
      if (f % 10000 == 0) {
        db.flush(new FlushOptions().setWaitForFlush(true))
      }
    })

  }


}
