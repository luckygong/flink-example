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

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.DataStream

/**
 * Simple example for demonstrating the use of SQL on a Stream Table.
 *
 * nc: `nc -lk 9999`
 *
 * after program running, visit: http://localhost:8081/#/overview
 */
object WebUIExample {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val conf = new Configuration
    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    conf.setInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT)
    val env = new org.apache.flink.streaming.api.scala.StreamExecutionEnvironment(
      org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.createLocalEnvironment(2, conf)
    )

    val stream: DataStream[String] = env.socketTextStream("localhost", 9999)
    stream.print()
    env.execute()
  }

  final case class News(content: String, ct: Long = System.currentTimeMillis())

}
