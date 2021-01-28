package utils

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * 读取resources路径下的配置文件
 */
object PropertiesUtil {
    def load(propertieName:String): Properties ={
        val prop = new Properties();
        prop.load(
            new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName),
                StandardCharsets.UTF_8))
        prop
    }

    /**
     * 使用说明
     * def main(args: Array[String]): Unit = {
        val properties: Properties =  PropertiesUtil.load("config.properties")
        println(properties.getProperty("kafka.broker.list"))
      }
     */
}