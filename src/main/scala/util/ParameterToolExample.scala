package util

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.java.utils.ParameterTool

/**
 * Example of [[ParameterTool]]
 * Created by sjk on 10/21/16.
 */
object ParameterToolExample {
  def main(args: Array[String]): Unit = {

    val param = ParameterTool.fromPropertiesFile("src/main/resources/log4j.properties")
    println(param.toMap)

    val cf = ConfigFactory.load("src/main/resources/log4j.properties")

  }


}
