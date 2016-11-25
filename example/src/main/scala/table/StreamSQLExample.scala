/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package table

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.TableEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.slf4j.LoggerFactory

/**
 * Simple example for demonstrating the use of SQL on a Stream Table.
 *
 * This example shows how to:
 * - Convert DataStreams to Tables
 * - Register a Table under a name
 * - Run a StreamSQL query on the registered Table
 *
 */
object StreamSQLExample {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val orderA: DataStream[Order] = env.fromCollection(Seq(Order(1L, "beer", 3), Order(1L, "diaper", 4), Order(3L, "rubber", 2)))
    val orderB: DataStream[Order] = env.fromCollection(Seq(Order(2L, "pen", 3), Order(2L, "rubber", 3), Order(4L, "beer", 1)))

    // register the DataStreams under the name "OrderA" and "OrderB"
    tEnv.registerDataStream("OrderA", orderA, 'user, 'product, 'amount, 'ct)
    tEnv.registerDataStream("OrderB", orderB, 'user, 'product, 'amount, 'ct)

    // union the two tables
    val result = tEnv.sql(
      """
        |SELECT STREAM * FROM OrderA WHERE amount > 2
        |UNION ALL
        |SELECT STREAM * FROM OrderB WHERE amount < 2
      """.stripMargin
    )
      .filter('amount > 0)
    result.toDataStream[Order].print()

    //    tEnv.sql(
    //      """
    //        |SELECT STREAM
    //        |  TUMBLE_START(ct, INTERVAL ‘1’ MIMUTE) AS minute,
    //        |  COUNT(*) AS cnt
    //        |FROM OrderA
    //        |WHERE
    //        |  product = ‘pen’
    //        |GROUP BY
    //        |  TUMBLE(ct, INTERVAL ‘1’ MINUTE)
    //      """.stripMargin)
    logger.info("========" + env.getExecutionPlan)
    env.execute()
  }

  case class Order(user: Long, product: String, amount: Int, ct: Long = System.currentTimeMillis())

}